#include "LogMgr.h"
#include <sstream>

using namespace std;

	//Dirty page table is a map from page_id to LSN
int LogMgr::getLastLSN(int txnum) {
	auto iter = tx_table.find(txnum);
	int size = tx_table.size();
	if (iter != tx_table.end()){
		return tx_table[txnum].lastLSN;
	} else {
		return NULL_LSN;
	}
}

void LogMgr::setLastLSN(int txnum, int lsn) {
	tx_table[txnum].lastLSN = lsn;
}

void LogMgr::flushLogTail(int maxLSN) {
	string logstring = "";
	vector <LogRecord*> newlogtail; 
	for(auto iter = logtail.begin(); iter != logtail.end(); ++iter) {
		if ((*iter)->getLSN() <= maxLSN) {
			logstring += (*iter)->toString();
		} else {
			newlogtail.push_back(*iter);
		}
	}
	se->updateLog(logstring);
	logtail = newlogtail;
}

void LogMgr::analyze(vector <LogRecord*> log) {
	//Put active transactions into txtable, put updates into dirty page table
	//Remove transactions from txtable when tranactiontype is end

	for(auto iter = log.begin(); iter != log.end(); ++iter) {
		//Clear transaction table entry when type is end:
		if ((*iter)->getType() == END) {
			int txid = (*iter)->getTxID();
			tx_table.erase(txid);
		}

		//Update dirty page table when type is update:
		UpdateLogRecord* ulr = dynamic_cast<UpdateLogRecord*>(*iter);
		if (ulr == nullptr) {
			auto dptIterator = dirty_page_table.find(ulr->getPageID());
			if (dptIterator == dirty_page_table.end()) {
				dirty_page_table[ulr->getPageID()] = ulr->getLSN();
			}
		}
	}
	//Find active transactions that were never commit-ended.
}

bool LogMgr::redo(vector <LogRecord*> log) {
	//UPDATE TXID STUFF STILL NOT COMPLETE - possibly complete
	//page,recLSN
	int earliestChange = -1;
	//Checks if table is 
	if(dirty_page_table.size() > 0){
		earliestChange = dirty_page_table.begin()->second;
	} else {
		return true;
	}
	for(auto iter = dirty_page_table.begin(); iter != dirty_page_table.end(); ++iter){
		if(earliestChange > iter->second){
			earliestChange = iter->second;
		}
	}

	auto iter = log.begin();
	for(; iter != log.end(); ++iter){
		if((*iter)->getLSN() == earliestChange) break;
	}

	for(; iter != log.end(); ++iter){
		if((*iter)->getType() == UPDATE){
			UpdateLogRecord* ulr = dynamic_cast<UpdateLogRecord*>(*iter);
			int ulrPage = ulr->getPageID();
			int ulrLSN = ulr->getLSN();
			int pageLSN = se->getLSN(ulrPage);
			if(dirty_page_table.find(ulrPage) == dirty_page_table.end() 
				|| dirty_page_table.find(ulrPage) != dirty_page_table.end() && dirty_page_table.find(ulrPage)->second > ulrLSN 
				|| pageLSN >= ulrLSN){
				continue;
			} else {
        		if(!se->pageWrite(ulrPage, ulr->getOffset(), ulr->getAfterImage(), ulrLSN)){
        			return false;
        		}

			}
		} else if((*iter)->getType() == CLR){
			CompensationLogRecord* clr = dynamic_cast<CompensationLogRecord*>(*iter);
			int clrPage = clr->getPageID();
			int clrLSN = clr->getLSN();
			int pageLSN = se->getLSN(clrPage);
			if(dirty_page_table.find(clrPage) == dirty_page_table.end()
				|| dirty_page_table.find(clrPage) != dirty_page_table.end() && dirty_page_table.find(clrPage)->second > clrLSN
				|| pageLSN >= clrLSN){
				continue;
			} else {
				if(!se->pageWrite(clrPage, clr->getOffset(), clr->getAfterImage(), clrLSN)){
        			return false;
        		}
			}
		}
	}

	for(auto iter = tx_table.begin(); iter != tx_table.end(); ++iter){
		if(iter->second.status == C){
			tx_table.erase(iter);
		}
	}

	return true;
}

void LogMgr::undo(vector <LogRecord*> log, int txnum) {
	//Looping backwards from end of iterator.
	//If txnum == -1, I go until the transaction table is empty.
	//If txnum != -1, will return as soon as the transaction table element is removed.
	int prev_lsn = NULL_LSN;
	for (auto iter = log.end()-1; tx_table.size(); --iter) {
		if (txnum != NULL_LSN && (*iter)->getTxID() != txnum) {
			continue;
		}

		UpdateLogRecord* ulr = dynamic_cast<UpdateLogRecord*>(*iter);
		if (ulr != nullptr) {
			continue;
		}

		//Create the compensation log record
		int current_lsn = se->nextLSN();
		int txid = ulr->getTxID();
		int page_id = ulr->getPageID();
  		int page_offset = ulr->getOffset();
  		string after_img = ulr->getBeforeImage();
  		int undo_next_lsn = ulr->getprevLSN();
		CompensationLogRecord* cmpRecord = new CompensationLogRecord(
			current_lsn, prev_lsn, txid, page_id, page_offset,
			after_img, undo_next_lsn);

		prev_lsn = current_lsn;
		logtail.push_back(cmpRecord);

		if (!se->pageWrite(page_id, page_offset, after_img, current_lsn)) {
			return;
		}

		//If applicable, remove from transaction table
		if ((*iter)->getprevLSN() == NULL_LSN) {
			tx_table.erase((*iter)->getTxID());
			if (txnum != NULL_LSN) {
				//Only undoing one transaction
				return;
			}
		}
	}
}

vector<LogRecord*> LogMgr::stringToLRVector(string logstring) {
	vector<LogRecord*> result;
	istringstream stream(logstring);
	string line;
	while (getline(stream, line)) {
		LogRecord* lr = LogRecord::stringToRecordPtr(line);
		result.push_back(lr);
	}
	return result; 
}

void LogMgr::abort(int txid) {
	undo(logtail, txid);
}

void LogMgr::checkpoint() {
	int beginCkptLSN = se->nextLSN();
	LogRecord* ckptStart = new LogRecord(beginCkptLSN, -1, -1, BEGIN_CKPT);
	logtail.push_back(ckptStart);

	int endCkptLSN = se->nextLSN();
	ChkptLogRecord* ckptEnd = new ChkptLogRecord(endCkptLSN, beginCkptLSN, -1, tx_table, dirty_page_table);
	logtail.push_back(ckptEnd);
}

void LogMgr::commit(int txid) {
	//Add commit record
	int prev_lsn = getLastLSN(txid);
	LogRecord* commitLogRecord = new LogRecord(se->nextLSN(), prev_lsn, txid, COMMIT);
	logtail.push_back(commitLogRecord);
	setLastLSN(txid, commitLogRecord->getLSN());

	
	//Flush log tail including that commit record
	flushLogTail(commitLogRecord->getLSN());

	//Add end record but dont flush it
	prev_lsn = getLastLSN(txid);
	LogRecord* endLogRecord = new LogRecord(se->nextLSN(), prev_lsn, txid, END);
	logtail.push_back(endLogRecord);
	setLastLSN(txid, endLogRecord->getLSN());
	tx_table.erase(txid);
}

void LogMgr::pageFlushed(int page_id) {
	dirty_page_table.erase(page_id);
	flushLogTail(se->getLSN(page_id));
}

void LogMgr::recover(string log) {
		//analyze(logtail);
		//if (redo()) {;
		//undo();
		//}
}

	//Write to memory
int LogMgr::write(int txid, int page_id, int offset, string input, string oldtext) {
		//Update tx_table:
	//tx_table[txid].lastLSN = getLastLSN(txid);
	//tx_table[txid].status = U;

	int lastLSNJank = getLastLSN(txid);
	tx_table[txid].lastLSN = lastLSNJank;
	tx_table[txid].status = U;

	//If relevant Update dirty_page_table:
	auto dptEntry = dirty_page_table.find(page_id);
	if (dptEntry == dirty_page_table.end()) {
		dirty_page_table[page_id] = se->getLSN(txid);
	}

	//Add a log about it:
	int prev_lsn = getLastLSN(txid);
	UpdateLogRecord* updateLogRecord = new UpdateLogRecord(se->nextLSN(), prev_lsn, txid, page_id, offset, oldtext, input);
	logtail.push_back(updateLogRecord);
	setLastLSN(txid, updateLogRecord->getLSN());

	return tx_table[txid].lastLSN;
}

void LogMgr::setStorageEngine(StorageEngine* engine) {
	se = engine;
}
