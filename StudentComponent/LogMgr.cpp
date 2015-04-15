#include "LogMgr.h"

using namespace std;

 	//Dirty page table is a map from page_id to LSN
  int LogMgr::getLastLSN(int txnum) {
  	auto iter = tx_table.find(txnum);
  	if (iter != tx_table.end()){
  		return iter->second.lastLSN;
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
  			logstring += (*iter)->toString() + "\n";
  		} else {
  			newlogtail.push_back(*iter);
  		}
  	}
  	se->updateLog(logstring);
  	logtail = newlogtail;
  }

  void LogMgr::analyze(vector <LogRecord*> log) {
  	for(auto iter = log.begin(); iter != log.end(); ++iter) {
      /*if (logrec.getType == TxType.END) {
        //clear from table
      }*/
      UpdateLogRecord* ulr = dynamic_cast<UpdateLogRecord*>(*iter);
  		if (ulr) {
        auto dptIterator = dirty_page_table.find(ulr->getPageID());
        if (dptIterator != dirty_page_table.end()) {
          dptIterator->second = ulr->getLSN();
        }
  		}
  	}
  }

  bool LogMgr::redo(vector <LogRecord*> log) {
    return true;
  }

  void LogMgr::undo(vector <LogRecord*> log, int txnum) {

  }

  vector<LogRecord*> LogMgr::stringToLRVector(string logstring) {
    return vector<LogRecord*>();
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
    //Flush log tail including that commit record
    flushLogTail(commitLogRecord->getLSN());
    //Add end record but dont flush it
    prev_lsn = getLastLSN(txid);
    LogRecord* endLogRecord = new LogRecord(se->nextLSN(), prev_lsn, txid, END);
    logtail.push_back(endLogRecord);
  }

  void LogMgr::pageFlushed(int page_id) {
    dirty_page_table.erase(se->getLSN(page_id));
    flushLogTail(se->getLSN(page_id));
  }

  void LogMgr::recover(string log) {

  }

  int LogMgr::write(int txid, int page_id, int offset, string input, string oldtext) {
    //Update tx_table:
    tx_table[txid].lastLSN = se->getLSN(page_id);
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

    return tx_table[page_id].lastLSN;
  }

  void LogMgr::setStorageEngine(StorageEngine* engine) {

  }