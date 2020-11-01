#include <bits/stdc++.h>
#include <fstream>
#include <pthread.h>
#include <unistd.h>
using namespace std;
// Actual Database with name mapped to value, eg. 'u' : 100
std::unordered_map<std::string, int> vars;

// Function to split a line from text file
std::vector<std::string> splitWord(std::string str)
{
	std::vector<std::string> res;
	std::string word = "";
	for (auto x : str)
	{
		if (x == ' ')
		{
			res.push_back(word);
			word = "";
		}
		else
		{
			word = word + x;
		}
	}
	// For eg. if str = u = u + v then res = [u,=,u,+,v]
	res.push_back(word);
	return res;
}
// Custom data struct to store operation details to be done on databse variable (u = u + 100 or u = u + v)
struct Operation
{
	std::string varname; //databaes variable
	std::string op;		 //operation char eg. R/W/C

	bool isOtherVar;	  //bool to tell if another variable involved in operation
	std::string otherVar; //to store that other var

	bool isVal; // bool to check if there is an int value involved in operation
	int value;	// if yes then to store that value

	// final assignment with passed arguments
	Operation(std::string s1, std::string c1, bool f1, bool f2, std::string s2, int v1)
	{
		varname = s1;
		op = c1;
		isOtherVar = f1;
		isVal = f2;
		otherVar = s2;
		value = v1;
	}
};

// Data struture to store lock request (R u or W u)
struct Request
{
	std::string type;	 //type of request
	std::string varname; //databse variable associated with request

	Request(std::string t, std::string v)
	{
		type = t;
		varname = v;
	}
};

// Transaction class to extract data for transaction
class Transaction
{
private:
	int tid;
	std::vector<Operation> opSeq; //vector to store all operations of a transaction
	std::vector<Request> reqSeq;  // vector to store lock requests
	std::vector<string> seq;			  //sequence string to store the sequence of what to be done ORORORRO
public:
	Transaction() {}

	Transaction(int id)
	{
		tid = id;
	}

	void addOperation(Operation O)
	{
		seq.push_back("O"); //O means perform operation
		opSeq.push_back(O);
	}

	void addRequest(Request R)
	{
		seq.push_back("R"); //R means lock request
		reqSeq.push_back(R);
	}

	void addResult(string s)
	{
		seq.push_back(s);
	}

	std::vector<Operation> getopSeq()
	{
		return opSeq;
	}
	std::vector<Request> getreqSeq()
	{
		return reqSeq;
	}
	std::vector<string> getseq()
	{
		return seq;
	}

	// Function to return Tid for a particular transaction
	int getId()
	{
		return tid;
	}
};

bool compareSets(unordered_set<int> a, unordered_set<int> b)
{
	if (a.size() != b.size())
		return 0;

	for (auto it1 = a.begin(), it2 = b.begin(); it1 != a.end(), it2 != b.end(); it1++, it2++)
	{
		if (*it1 != *it2)
			return 0;
	}

	return 1;
}

// Lockmngr class for locking functions
class LockMgr
{
private:
	pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER; //single lock for all

	std::unordered_map<std::string, pthread_cond_t> condvar; //condvar for each database var
	std::unordered_map<std::string, unordered_set<int>> active_read;		 //active readers on each database var
	std::unordered_map<std::string, unordered_set<int>> active_write;		 // active writers on each database var
	std::unordered_map<std::string, unordered_set<int>> waiting_read;		 // waiting readers on each database var
	std::unordered_map<std::string, unordered_set<int>> waiting_write;		 // waiting witers on each database var
	std::unordered_map<std::string, std::queue<Transaction>> Q;
	//mp keeps track of what lock is held by what tid
	std::unordered_map<int, unordered_map<string, int>> mp; // 0  for not present, 1 for read-only, 2 for write-read

public:
	// Initialize everything
	LockMgr()
	{
		lock = PTHREAD_MUTEX_INITIALIZER;
		for (auto itr = vars.begin(); itr != vars.end(); itr++)
		{
			condvar[itr->first] = PTHREAD_COND_INITIALIZER;
		}
	}
	// for R
	bool acquireReadLock(int tid, std::string varname)
	{
		pthread_mutex_lock(&lock);

		while (active_write[varname].size() > 0 || waiting_write[varname].size() > 0)
		{
			waiting_read[varname].insert(tid);
			pthread_cond_wait(&condvar[varname], &lock);
			waiting_read[varname].insert(tid);
		}

		mp[tid][varname] = 1;
		active_read[varname].insert(tid);
		pthread_mutex_unlock(&lock);

		return true;
	}
	// for W
	bool acquireWriteLock(int tid, std::string varname)
	{
		pthread_mutex_lock(&lock);

		while (active_write[varname].size() > 0 || active_read[varname].size() > 0)
		{
			waiting_write[varname].insert(tid);
			pthread_cond_wait(&condvar[varname], &lock);
			waiting_write[varname].erase(tid);
		}

		mp[tid][varname] = 2;
		active_write[varname].insert(tid);
		pthread_mutex_unlock(&lock);

		return true;
	}
	// for R -> W
	bool upgradeToWrite(int tid, std::string varname)
	{
		if (mp[tid][varname] != 1)
			return false;

		pthread_mutex_lock(&lock);
		
		while (active_read[varname].size() > 1 || active_write[varname].size() > 0)
		{
			waiting_write[varname].insert(tid);
			pthread_cond_wait(&condvar[varname], &lock);
			waiting_write[varname].erase(tid);
		}

		mp[tid][varname] = 2;
		active_read[varname].erase(tid);
		active_write[varname].insert(tid);
		pthread_mutex_unlock(&lock);

		return true;
	}
	// to release lock
	bool releaseLock(int tid, std::string varname)
	{
		pthread_mutex_lock(&lock);

		if (mp[tid][varname] == 2)
		{
			mp[tid][varname] = 0;
			active_write[varname].erase(tid);
			pthread_cond_broadcast(&condvar[varname]);
		}
		else if (mp[tid][varname] == 1)
		{
			mp[tid][varname] = 0;
			active_read[varname].erase(tid);
			if (active_read[varname].size() == 0 || (active_read[varname].size() > 0 && compareSets(active_read[varname], waiting_write[varname]))) 
			{
				pthread_cond_broadcast(&condvar[varname]);
			}
		}

		pthread_mutex_unlock(&lock);

		return true;
	}
};

LockMgr *locker;
// Function to check if string contains a digit
bool isNumber(std::string s)
{
	for (int i = 0; i < s.size(); i++)
		if (isdigit(s[i]) == false)
			return false;

	return true;
}

// Execution function for each transaction


void *runTransaction(void *T)
{
	Transaction *trx;
	// trx is current transaction instance
	trx = (Transaction *)T;
	// Get all data related to this transaction
	std::vector<Operation> opseq = trx->getopSeq();
	std::vector<Request> reqseq = trx->getreqSeq();
	
	vector<string> seq = trx->getseq();
	
	std::unordered_map<std::string, int> map1; //This map is local copy of database, finally this will written to global
	std::unordered_map<std::string, int> map2; //This map stores value after performing operation on var
	
	int req_counter = 0, op_counter = 0;

	printf("Transaction id: %d\n", trx->getId());

	vector<string> varsWithWriteLock;

	for (int i = 0; i < seq.size(); i++)
	{
		// If there is request in seq
		cout << trx->getId() << " " << seq[i] << endl;
		if (seq[i] == "R")
		{
			// if it is a read request
			if (reqseq[req_counter].type == "R")
			{
				locker->acquireReadLock(trx->getId(), reqseq[req_counter].varname);
				map1[reqseq[req_counter].varname] = vars[reqseq[req_counter].varname];
				map2[reqseq[req_counter].varname] = vars[reqseq[req_counter].varname];
			}
			// If it is a W request
			else
			{
				map1[reqseq[req_counter].varname] = map2[reqseq[req_counter].varname];
			}
			req_counter++;
		}
		// If there is an Operation in seq
		else if (seq[i] == "O")
		{	
			// Check if already have read lock
			if (locker->upgradeToWrite(trx->getId(), opseq[op_counter].varname))
			{
				// Check if there is other variable present
				varsWithWriteLock.push_back(opseq[op_counter].varname);
				if (opseq[op_counter].isOtherVar)
				{
					if (opseq[op_counter].op == "+")
					{
						map2[opseq[op_counter].varname] = map2[opseq[op_counter].varname] + map1[opseq[op_counter].otherVar];
					}
					else
					{
						map2[opseq[op_counter].varname] = map2[opseq[op_counter].varname] - map1[opseq[op_counter].otherVar];
					}
				}
				else
				{
					if (opseq[op_counter].op == "+")
					{
						map2[opseq[op_counter].varname] = map2[opseq[op_counter].varname] + opseq[op_counter].value;
					}
					else
					{
						map2[opseq[op_counter].varname] = map2[opseq[op_counter].varname] - opseq[op_counter].value;
					}
				}
			}
			// If not then acquire write lock
			else
			{
				locker->acquireWriteLock(trx->getId(), opseq[op_counter].varname);
				varsWithWriteLock.push_back(opseq[op_counter].varname);
				if (opseq[op_counter].isOtherVar)
				{
					if (opseq[op_counter].op == "+")
					{
						map2[opseq[op_counter].varname] = map2[opseq[op_counter].varname] + map1[opseq[op_counter].otherVar];
					}
					else
					{
						map2[opseq[op_counter].varname] = map2[opseq[op_counter].varname] - map1[opseq[op_counter].otherVar];
					}
				}
				else
				{
					if (opseq[op_counter].op == "+")
					{
						map2[opseq[op_counter].varname] = map2[opseq[op_counter].varname] + opseq[op_counter].value;
					}
					else
					{
						map2[opseq[op_counter].varname] = map2[opseq[op_counter].varname] - opseq[op_counter].value;
					}
				}
			}
			op_counter++;
		}
		else if (seq[i] == "C")
		{
			break;
		}
		else if (seq[i] == "A")
		{
			goto here;
		}
	}
	// After completion of trx i.e C, write map1 to global
	for (auto str : varsWithWriteLock)
	{
		vars[str] = map1[str];
	}

	req_counter = op_counter = 0;
		
	here:;
	for (auto itr = vars.begin(); itr != vars.end(); itr++)
	{
		locker->releaseLock(trx->getId(), itr->first);
	}
	return NULL;
}

int main()
{
	std::string text;
	std::ifstream file("input.txt");

	getline(file, text); // number of transactions N
	std::vector<std::string> temp = splitWord(text);

	int N = stoi(temp[0]);

	getline(file, text); // variables and their values
	temp = splitWord(text);
	// Put the initial values
	for (int i = 0; i < temp.size(); i++)
	{
		if (i % 2)
		{
			vars[temp[i - 1]] = stoi(temp[i]);
		}
		else
		{
			vars[temp[i]] = 0;
		}
	}
	cout << "initial Data: "
		 << "\n\n";
	for (auto i : vars)
	{
		cout << i.first << " = " << i.second << "\n";
	}

	// array for each transaction
	Transaction tarr[N];

	int counter = 0;
	// fill data in that array
	getline(file, text);
	while (counter < N)
	{
		int tid = stoi(text);

		Transaction T(tid);
		while (text != "" && text != "C")
		{
			getline(file, text);

			if (text == "C" || text == "A")
			{
				T.addResult(text);
				while (1)
				{
					getline(file, text);
					if (isNumber(text))
						break;
				}
				break;
			}

			std::vector<std::string> vec = splitWord(text);

			if (vec.size() == 2) // This means that line is smthing like "R u"
			{
				Request R(vec[0], vec[1]);
				T.addRequest(R);
			}
			// Else it is a math operation like u = u + v
			else
			{
				// check if last thing is a number or var
				bool f = isNumber(vec.back());
				int num;

				if (f)
					num = std::stoi(vec[4]);
				else
					num = 0;
				// store the operation
				Operation O(vec[0], vec[3], !f, f, vec[4], num);
				T.addOperation(O);
			}
		}
		// Store all the info for this transaction in array and move to nxt
		tarr[counter++] = T;
	}

	locker = new LockMgr();


	
	// Now execute every transction
	pthread_t threads[N];
	for (int i = 0; i < N; i++)
	{
		pthread_create(&threads[i], NULL, runTransaction, (void *)&tarr[i]);
	}
	for (int i = 0; i < N; i++)
	{
		pthread_join(threads[i], NULL);
	}
	cout << "final Data: "
		 << "\n";
	for (auto i : vars)
	{
		cout << i.first << " = " << i.second << "\n";
	}
	std::cout << "The End"
			  << "\n";
	file.close();
}
