#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <string>
#include <sys/stat.h>
#include <string.h>
#include <string>
#include <iostream>
#include <sstream>
#include <sys/mman.h>  
#include <algorithm>
#include <stdint.h>
#include <functional>
#include <list>
#include <vector>
#include <mutex>
#include <unordered_map>
#include <memory>
#include <assert.h>

/*
基本思路 ：
为4T数据建立哈希表，但是粗劣估算，整个哈希表也需要1T以上的空间，但是内存只有4G（2-3 G可用），磁盘有3T可用
所以只能把哈希表放入磁盘。

1，遍历1T大文件，解析出kv，根据key长度，顺序写入临时文件，场景中，key是1-1024B，所以会有最多1024个临时文件,
   临时文件中的数据皆为 key-val_offset-val_size。
2，用临时文件建立哈希表，哈希表用开放定址法，因为同一个临时文件key都是一样的，hash表的元素大小也一样，可以
   直接使用取模进行定位。
3，内存中使用lru cache缓存hash表的shard。

性能分析 ：
索引建立好之后，在缓存命中情况，只需访问一次硬盘(读数据)。如果没有命中，则平均只访问2次硬盘(读硬盘)。

因为时间紧迫，还有没完成的工作:
1, 多线程并行建立hash文件
2，系统api的封装
3, 完整的性能测试
4, 为了方便编码，把所有类放在一起，没有划分
5, 建立索引的时候，崩溃需要重来，做错误恢复。

最后感谢TIDB给的一次机会，让我在这次小作业学习了很多文件存储相关的知识
编译 : g++ main.cpp -o main --std=c++11
*/

using namespace std;

const int MAX_KEY_SIZE = 1024;
const uint64_t PARTITION_SIZE = 1024 * 1024 * 32;

struct FileNameTool {
	static string temp_file_name(string dir, int index);
	static string hash_index_file_name(string dir, int index);
};

string FileNameTool::temp_file_name(string dir, int index) {
	stringstream ss;
	ss << dir << "/tmp_" << index;
	return ss.str();
}

string FileNameTool::hash_index_file_name(string dir, int index) {
	stringstream ss;
	ss << dir << "/hash_index_" << index;
	return ss.str();
}

class IndexBuilder {
public:
    IndexBuilder(const char* _super_block_name, uint64_t _partition_size, string _index_dir = "index");
	~IndexBuilder();
    void init_temporary_files();
	void init_hash_index_files();

private:
	 void write_temporary_file(const string& key, uint64_t val_offset, uint64_t val_size);
	 void build_hash_table(int hi_fd, int key_size, uint64_t hash_slot_size, uint64_t hash_slot_count);

private:
	hash<string> hash_function;
    string super_block_name;
	string index_dir;
    uint64_t partition_size;
	uint64_t super_block_offset;
    int temporary_files_fds[MAX_KEY_SIZE + 1];
	uint64_t hash_index_element_count[MAX_KEY_SIZE + 1];
    int sb_fd;
};

IndexBuilder::IndexBuilder(const char* _super_block_name, uint64_t _partition_size, string _index_dir) :
	super_block_name(_super_block_name),
	index_dir(_index_dir),
	partition_size(_partition_size),
	super_block_offset(0)
{
	if (index_dir.empty()) {
		assert(0);
	}
	struct stat st = { 0 };
	if (stat(index_dir.c_str(), &st) == -1) {
		mkdir(index_dir.c_str(), 0755);
	}

	memset(temporary_files_fds, -1, sizeof(int) * (MAX_KEY_SIZE + 1));
	memset(hash_index_element_count, 0, sizeof(uint64_t) * (MAX_KEY_SIZE + 1));
}

IndexBuilder::~IndexBuilder() {
	for (int i = 1; i < MAX_KEY_SIZE + 1; i++) {
		if (temporary_files_fds[i] > 0) {
			close(temporary_files_fds[i]);
		}
	}
}

void IndexBuilder::init_temporary_files() {
	int sb_fd;
	sb_fd = open(super_block_name.c_str(), O_RDONLY);
    if(sb_fd < 0) {
        cout << "ERROR: open spuerblock error file_name=" << super_block_name << endl;
        return;
    }

	struct stat stat_buf;
	int rc = stat(super_block_name.c_str(), &stat_buf);
	if (rc < 0) {
		cout << "ERROR: stat file error super_block_name=" << super_block_name << endl;
		return;
	}

	char *content_buf = new char[partition_size];
	uint64_t key_size, val_size;
	string key;
	//为了减少io调用，每次读取 partition_size 的内存，再进行解析，这里需要处理
	//的一种情况就是，读的内存切好不够解析一个字段，我直接不更新offset，只有读完
	//一整key-value才进行更新
    for(; super_block_offset < stat_buf.st_size;) {
		lseek(sb_fd, super_block_offset, SEEK_SET);
		uint64_t read_len = min(partition_size, stat_buf.st_size - super_block_offset);
		read(sb_fd, content_buf, read_len);
		for (uint64_t ofs = 0; ofs < read_len;) {
			if (read_len - ofs < sizeof(uint64_t)) {
				break;
			}
			memcpy(&key_size, content_buf + ofs, sizeof(uint64_t));
			ofs += sizeof(uint64_t);

			if (read_len - ofs < key_size) {
				break;
			}
			key.assign(content_buf + ofs, key_size);
			ofs += key_size;

			if (read_len - ofs < sizeof(uint64_t)) {
				break;
			}
			memcpy(&val_size, content_buf + ofs, sizeof(uint64_t));
			ofs += (sizeof(uint64_t) + val_size);

			uint64_t val_offset = super_block_offset + sizeof(uint64_t) + key_size + sizeof(uint64_t);
			super_block_offset = val_offset + val_size;
			write_temporary_file(key, val_offset, val_size);
			hash_index_element_count[key_size]++;
		}
    }

	close(sb_fd);
	delete content_buf;
}

void IndexBuilder::write_temporary_file(const string& key, uint64_t val_offset, uint64_t val_size) {
	if (temporary_files_fds[key.size()] < 0) {
		string name = FileNameTool::temp_file_name(index_dir, key.size());
		temporary_files_fds[key.size()] = open(name.c_str(), O_RDWR | O_APPEND | O_CREAT, 0666);
		if (temporary_files_fds[key.size()] < 0) {
			cout << "ERROR: open temp file error file_name=" << name << endl;
			return;
		}
	}
	string content;
	char buf[8];
	char *p = buf;
	content.append(key.c_str(), key.size());
	memcpy(p, &val_offset, sizeof(val_offset));
	content.append(p, sizeof(uint64_t));
	memcpy(p, &val_size, sizeof(val_size));
	content.append(p, sizeof(uint64_t));
	//TODO fix use a buffer flush 
	write(temporary_files_fds[key.size()], content.c_str(), content.size());
}

void IndexBuilder::init_hash_index_files() {
	for (int key_size = 1; key_size < MAX_KEY_SIZE + 1; key_size++) {
		if (temporary_files_fds[key_size] < 0) {
			cout << "INFO: key size = " << key_size << "not exist" << endl;
			continue;
		}

		int hi_fd;
		string name = FileNameTool::hash_index_file_name(index_dir, key_size);
		hi_fd = open(name.c_str(), O_RDWR | O_CREAT, 0666);
		if (hi_fd < 0) {
			cout << "ERROR: open hash index file error file_name=" << super_block_name << endl;
			return;
		}

		uint64_t hash_slot_size  = (uint64_t)key_size + 2 * sizeof(uint64_t); // hash_element = key + val_offset + val_size
		uint64_t hash_slot_count = hash_index_element_count[key_size] / 0.75;
		lseek(hi_fd, (off_t)(hash_slot_count * hash_slot_size) - 1, SEEK_END);
		write(hi_fd, "0", 1);
		build_hash_table(hi_fd, key_size, hash_slot_size, hash_slot_count);
		close(hi_fd);
	}
}

void IndexBuilder::build_hash_table(int hi_fd, int key_size, uint64_t hash_slot_size, uint64_t hash_slot_count) {
	//read 1024 keys each time
	uint64_t patition = 1024;
	int fd = temporary_files_fds[key_size];
	char *buf = new char[patition * hash_slot_size];
	char *slot_key_buf = new char[key_size];
	string empty_hash_element(key_size, '\0');
	string key;
	string slot_key;

	for (int cnt = 0; cnt < hash_index_element_count[key_size];) {
		lseek(fd, cnt * hash_slot_size, SEEK_SET);
		uint64_t delta_cnt = min(patition, hash_index_element_count[key_size] - cnt);
		read(fd, buf, delta_cnt * hash_slot_size);

		for (int i = 0; i < delta_cnt; i++) {
			key.assign(buf + i * hash_slot_size, key_size);
			uint64_t slot = hash_function(key) % hash_slot_count;
			do {
				lseek(hi_fd, slot * hash_slot_size, SEEK_SET);
				read(hi_fd, slot_key_buf, key_size);
				slot_key.assign(slot_key_buf, key_size);
				if (key == slot_key || slot_key == empty_hash_element) {
					lseek(hi_fd, slot * hash_slot_size, SEEK_SET);
					write(hi_fd, buf + i * hash_slot_size, hash_slot_size);
					break;
				} else {
					slot = (slot + 1) % hash_slot_count;
				}
			} while (1);
		}

		//update cnt
		cnt += delta_cnt;
	}
	delete[] buf;
	delete[] slot_key_buf;
}


//--------------------------------LRU IMPLEMENT-----------------------------//
class LRUCache {
public:
	LRUCache(int _lru_shard, int _queue_size);
	~LRUCache();
	void put(uint64_t hash_shard_id, char* p);
	char* get(uint64_t hash_shard_id);

	struct  Queue {
		unique_ptr<mutex> mtx;
		list<pair<uint64_t, char*> > queue;

		Queue() : mtx(new mutex) {}
	};

private:
	int lru_shard;
	int max_queue_size;
	vector<Queue> queues;
};

LRUCache::LRUCache(int _lru_shard, int _max_queue_size) :
	lru_shard(_lru_shard),
	max_queue_size(_max_queue_size)
{
	for (int i = 0; i < lru_shard; i++) {
		Queue q;
		queues.push_back(std::move(q));
	}
}

LRUCache::~LRUCache() {
	for (int i = 0; i < lru_shard; i++) {
		lock_guard<mutex> guard(*queues[i].mtx);
		for (auto it = queues[i].queue.begin(); it != queues[i].queue.end(); it++) {
			delete[] it->second;
		}
	}
}

void LRUCache::put(uint64_t hash_shard_id, char* p) {
	int n = hash_shard_id % lru_shard;
	lock_guard<mutex> guard(*queues[n].mtx);
	queues[n].queue.push_front(make_pair(hash_shard_id, p));
	if (queues[n].queue.size() > max_queue_size) {
		auto p = queues[n].queue.back();
		queues[n].queue.pop_back();
		delete[] p.second;
	}
}

char* LRUCache::get(uint64_t hash_shard_id) {
	int n = hash_shard_id % lru_shard;
	lock_guard<mutex> guard(*queues[n].mtx);
	char *p = NULL;
	for (auto it = queues[n].queue.begin(); it != queues[n].queue.end(); it++) {
		if (it->first == hash_shard_id) {
			queues[n].queue.erase(it);
			queues[n].queue.push_front(*it);
			p = it->second;
			break;
		}
	}
	return p;
}


//--------------------------------READER IMPLEMENT-----------------------------//
class KvReader {
public:
	KvReader(string super_block_name, string dir_name = "index");
	~KvReader();

	bool get(const string& key, string& val);
	bool get_index(const string& key, uint64_t* offset, uint64_t* size);
	int read_index_from_hash_shard(char *p, uint64_t hash_slot_size, uint64_t from, 
		const string& key, uint64_t* offset, uint64_t* size);

private:
	int sb_fd;
	int hash_index_fds[MAX_KEY_SIZE + 1];
	uint64_t hash_shard_slot_count;
	size_t hash_index_file_size[MAX_KEY_SIZE + 1];
	string super_block_name;
	string dir_name;
	hash<string> hash_function;
	unordered_map<int, unique_ptr<LRUCache> > lru_cache;
};

KvReader::KvReader(string _super_block_name, string _dir_name) :
	super_block_name(_super_block_name), 
	dir_name(_dir_name),
	hash_shard_slot_count(32)
{
	sb_fd = open(super_block_name.c_str(), O_RDONLY);
	if (sb_fd < 0) {
		cout << "ERROR: open spuerblock error file_name=" << super_block_name << endl;
		return;
	}

	memset(hash_index_fds, -1, sizeof(int) * (MAX_KEY_SIZE + 1));
	for (int key_size = 1; key_size < MAX_KEY_SIZE + 1; key_size++) {
		string index_file_name = FileNameTool::hash_index_file_name(dir_name, key_size);
		hash_index_fds[key_size] = open(index_file_name.c_str(), O_RDONLY);
		if (hash_index_fds[key_size] < 0) {
			cout << "INFO: index_file not exist, name=" << index_file_name << endl;
			continue;
		}

		// get hash file size
		struct stat stat_buf;
		int rc = stat(index_file_name.c_str(), &stat_buf);
		if (rc < 0) {
			cout << "ERROR: stat file error name=" << index_file_name << endl;
			assert(0);
		}
		hash_index_file_size[key_size] = stat_buf.st_size;

		// queue_num 表示长度相同的key的cache的队列数目，这里每个长度相同的key限制使用2M内存
		// 因为保守估计可能只能使用2G内存. queue_size = 2M / (hash_slot_size * queue_num * hash_shard_slot_count) 算出每个队列装多少个shard
		int queue_num = 8;
		int hash_slot_size = (uint64_t)key_size + 2 * sizeof(uint64_t);
		int queue_size = 1024 * 1024 * 2 / (hash_slot_size * queue_num * hash_shard_slot_count);
		unique_ptr<LRUCache> p(new LRUCache(queue_num, queue_size));
		lru_cache[key_size] = std::move(p);
	}
}

KvReader::~KvReader() {
	close(sb_fd);
	for (int i = 1; i < MAX_KEY_SIZE + 1; i++) {
		if (hash_index_fds[i] > 0) {
			close(hash_index_fds[i]);
		}
	}
}

bool KvReader::get(const string& key, string& val) {
	uint64_t offset, size;
	if (get_index(key, &offset, &size)) {
		char *val_buf = new char[size];
		lseek(sb_fd, offset, SEEK_SET);
		read(sb_fd, val_buf, size);
		val.assign(val_buf, size);
		cout << "INFO: k-v found key=" << key << ", val=" << val << endl;
		delete[] val_buf;
		return true;
	}
	return false;
}

/*
hash_shard_slot_count 表示hash的shard分片的slot个数。
如果hash slot不是hash_shard_slot_count 整数倍，则从尾到头的个hash_shard_slot_count算最后一个分片
比如有个32 + 16个slot的hash，0-31 属于第一个shard，16-47为第二个shard

开放定址法有元素堆积的问题，但是这里没问题，因为是从硬盘读一个shard出来，所有大概率会一次读盘命中。
*/
bool KvReader::get_index(const string& key, uint64_t* offset, uint64_t* size) {
	int fd = hash_index_fds[key.size()];
	uint64_t hash_slot_size = (uint64_t)key.size() + 2 * sizeof(uint64_t);
	uint64_t slot_cnt = hash_index_file_size[key.size()] / hash_slot_size;
	size_t slot = hash_function(key) % slot_cnt;
	uint64_t hash_shard_id = slot / hash_shard_slot_count;
	uint64_t max_shard_id = slot_cnt / hash_shard_slot_count;
	uint64_t hash_shard_size = hash_shard_slot_count * hash_slot_size;
	uint64_t shard_offset = (hash_shard_id == max_shard_id ?
		hash_index_file_size[key.size()] - hash_shard_size : hash_shard_id * hash_shard_size);
	uint64_t read_cache_from = slot - shard_offset / hash_slot_size;
	int ret = 0;


	//get index from lru cache
	char *p = lru_cache[key.size()]->get(hash_shard_id);
	if (p) {
		ret = read_index_from_hash_shard(p, hash_slot_size, read_cache_from, key, offset, size);
		if (ret == 0) {
			cout << "INFO: find index in cache, key=" << key << ", offset=" << *offset << ", size=" << *size << endl;
			return true;
		}
	}

	//not in cache, find index in file
	p = new char[hash_shard_size];
	do {
		lseek(fd, shard_offset, SEEK_SET);
		read(fd, p, hash_shard_size);
		ret = read_index_from_hash_shard(p, hash_slot_size, read_cache_from, key, offset, size);
		if(ret == 0) {
			cout << "INFO: find index in file, put it to cache, key=" << key << ", offset=" << *offset << ", size=" << *size << endl;
			lru_cache[key.size()]->put(hash_shard_id, p);
			return true;
	    }
		hash_shard_id = (hash_shard_id + 1) % max_shard_id;
		shard_offset = (hash_shard_id == max_shard_id ?
			hash_index_file_size[key.size()] - hash_shard_size : hash_shard_id * hash_shard_size);
	} while (ret == -EAGAIN);

	delete[] p;
	return false;
}

int KvReader::read_index_from_hash_shard(char *p, uint64_t hash_slot_size, uint64_t from, 
		const string& key, uint64_t* offset, uint64_t* size) {
	string tmp_key;
	string empty_key(key.size(), '\0');
	for (uint64_t i = from; i < hash_shard_slot_count; i++) {
		tmp_key.assign(p + i * hash_slot_size, key.size());
		if (tmp_key == empty_key) {
			return -ENOENT;
		}

		if (tmp_key == key) {
			memcpy(offset, p + i * hash_slot_size + key.size(), sizeof(uint64_t));
			memcpy(size, p + i * hash_slot_size + key.size() + sizeof(uint64_t), sizeof(uint64_t));
			return 0;
		}
	}
	return -EAGAIN;
}

//测试代码
int main(int argc, const char *argv[]) {
	if (argc != 2) {
		return 0;
	}

	if (strcmp(argv[1], "w") == 0) {
		IndexBuilder index_builder("superblock", 1024 * 8);
		index_builder.init_temporary_files();
		index_builder.init_hash_index_files();
	}
	else if (strcmp(argv[1], "r") == 0) {
		KvReader reader("superblock");
		string val;
		if (reader.get("4", val))
		{
			cout << val << endl;
		}
		reader.get("4", val);
	}
	return 0;
}
