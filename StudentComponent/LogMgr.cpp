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
			int LSN = (*iter)->getLSN();
			tx_table.erase(LSN);
		}

		//Update dirty page table when type is update:
		UpdateLogRecord* ulr = dynamic_cast<UpdateLogRecord*>(*iter);
		if (ulr) {
			auto dptIterator = dirty_page_table.find(ulr->getPageID());
			if (dptIterator == dirty_page_table.end()) {
				dirty_page_table[ulr->getPageID()] = ulr->getLSN();
			}
		}
	}
	//Find active transactions that were never commit-ended.
}

bool LogMgr::redo(vector <LogRecord*> log) {
	return true;
}

void LogMgr::undo(vector <LogRecord*> log, int txnum) {

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

}

void LogMgr::checkpoint() {

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
		//redo();
		//undo();
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
		dirty_page_table[page_id] = se->getLSN(page_id);
	}
		//Add a log about it:
	int prev_lsn = getLastLSN(txid);
	UpdateLogRecord* updateLogRecord = new UpdateLogRecord(se->nextLSN(), prev_lsn, txid, page_id, offset, oldtext, input);
	logtail.push_back(updateLogRecord);
	setLastLSN(txid, updateLogRecord->getLSN());

	return tx_table[page_id].lastLSN;
}

void LogMgr::setStorageEngine(StorageEngine* engine) {
	se = engine;
}
