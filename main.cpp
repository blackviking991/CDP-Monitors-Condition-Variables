#include <bits/stdc++.h>
#include <fstream>
#include <pthread.h> 
#include <unistd.h>

std::unordered_map<std::string, int> vars;

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

	res.push_back(word); 
	return res;
}

struct Operation
{
	std::string varname;
	std::string op;
	
	bool isOtherVar;
	std::string otherVar;

	bool isVal;
	int value;

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

struct Request
{
	std::string type;
	std::string varname;

	Request(std::string t, std::string v)
	{
		type = t;
		varname = v;
	}
};

class Transaction 
{
private:
	int tid;
	std::vector<Operation> opSeq;
	std::vector<Request> reqSeq;
	std::string seq;
public:
	Transaction(){}

	Transaction(int id)
	{
		tid =  id;
		seq = "";
	}

	void addOperation(Operation O)
	{
		seq.push_back('O');
		opSeq.push_back(O);
	}

	void addRequest(Request R)
	{
		seq.push_back('R');
		reqSeq.push_back(R);
	}

	int getId()
	{
		return tid;
	}
};

class LockMgr
{
private:
	pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

	std::unordered_map<std::string, pthread_cond_t> condvar;
	std::unordered_map<std::string, int> active_read;
	std::unordered_map<std::string, int> active_write;
	std::unordered_map<std::string, int> waiting_read;
	std::unordered_map<std::string, int> waiting_write;
	std::unordered_map<std::string, std::queue<Transaction>> Q;

	std::unordered_map<int, int> mp; // 0  for not present, 1 for read-only, 2 for write-read

public: 
	LockMgr()
	{
		lock = PTHREAD_MUTEX_INITIALIZER;
		for (auto itr = vars.begin(); itr != vars.end(); itr++)
		{
			condvar[itr->first] = PTHREAD_COND_INITIALIZER;
		}
	}

	void acquireReadLock(int tid, std::string varname)
	{
		pthread_mutex_lock(&lock); 

		while (active_write[varname] > 0)
		{
			waiting_read[varname]++;
			pthread_cond_wait(&condvar[varname], &lock); 
			waiting_read[varname]--;
		}

		mp[tid] = 1;
		active_read[varname]++;
		pthread_mutex_unlock(&lock);
	}

	void acquireWriteLock(int tid, std::string varname)
	{
		pthread_mutex_lock(&lock); 

		while (active_write[varname] > 0 || active_read[varname] > 0)
		{
			waiting_write[varname]++;
			pthread_cond_wait(&condvar[varname], &lock); 
			waiting_write[varname]--;
		}

		mp[tid] = 2;
		active_write[varname]++;
		pthread_mutex_unlock(&lock);
	}

	void upgradeToWrite(int tid, std::string varname)
	{
		if (mp[tid] != 1)
			return;

		pthread_mutex_lock(&lock); 

		while (active_write[varname] > 1 || active_read[varname] > 0)
		{
			waiting_write[varname]++;
			pthread_cond_wait(&condvar[varname], &lock);
			waiting_write[varname]--;
		}

		mp[tid] = 2;
		active_read[varname]--;
		active_write[varname]++;
		pthread_mutex_unlock(&lock);
	}

	void releaseLock(int tid, std::string varname)
	{
		pthread_mutex_lock(&lock); 

		if (mp[tid] == 2)
		{
			mp[tid] = 0;
			active_write[varname]--;
			pthread_cond_broadcast(&condvar[varname]);
		}
		else if (mp[tid] == 1)
		{
			mp[tid] = 0;
			active_read[varname]--;
			if (active_read[varname] == 0 && waiting_write[varname] > 0)
			{
				pthread_cond_broadcast(&condvar[varname]);
			}
		}

		pthread_mutex_unlock(&lock);
	}
};

bool isNumber(std::string s)
{
    for (int i = 0; i < s.size(); i++)
        if (isdigit(s[i]) == false)
            return false;
 
    return true;
}

void* runTransaction ()
{
	// std::cout << T.getId() << " ";
	return NULL;
}

LockMgr* locker;

int main()
{
	std::string text;
	std::ifstream file("input.txt");

	getline (file, text); // number of transactions
	std::vector<std::string> temp = splitWord(text);

	int N = stoi(temp[0]);

	getline (file, text); // variables and their values
	temp = splitWord(text);
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

	locker = new LockMgr();

	Transaction tarr[N];

	int counter = 0;
	while (counter < N) 
	{
		getline (file, text);
		int tid = stoi(text);

		Transaction T(tid);
		while (text != "" && text != "C")
		{
			getline (file, text);

			if (text == "C")
				break;

			std::vector<std::string> vec = splitWord(text);

			if (vec.size() == 2)
			{
				Request R(vec[0], vec[1]);
				T.addRequest(R);
			}
			else
			{
				bool f = isNumber(vec.back());
				int num;

				if (f)
					num = std::stoi(vec[4]);
				else
					num = 0;

				Operation O(vec[0], vec[3], !f, f, vec[4], num);
				T.addOperation(O);
			}
		}

		tarr[counter++] = T;
	}

	pthread_t threads[N]; 
	for (int i = 0; i < N; i++)
	{
		pthread_create(&threads[i], NULL, runTransaction, NULL); 
	}

	// for (int i = 0; i < N; i++)
	// {
	// 	pthread_join(threads[i], NULL);
	// }

	file.close();
}