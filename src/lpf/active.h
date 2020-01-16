/*
 * create on: Oct 11, 2019
 *	  Auther: Pengfei Li
 */

#ifndef ACTIVE_H_
#define ACTIVE_H_

#include "../destor.h"
#include "../utils/lru_cache.h"
#include "../utils/sync_queue.h"
#include "../recipe/recipestore.h"
#include "../jcr.h"

struct cache_entry{
	fingerprint fp;
	int32_t flag;
	int32_t size;
	containerid id;
	//unsigned int *pages;
};

struct bitmap {
	int max_size;
	int ideal_size;

	int int_number;
	unsigned int *bitmap;
};

struct doubleBuffer {
	GHashTable *t1;
	GHashTable *t2;

	int t1size;
	int t2size;
};

struct recipeinfo {
	FILE *recipe_fp;
	int64_t number_of_chunks;

	char* segmentbuf;
	int segmentlen;
	int segmentbufoff;
};

struct doubleBuffer *new_double_buffer();

void statistic_index(struct segment* s);
void free_entry(void* ce);
int cache_entry_equal(void *ce, void *fp);

void init_active();
void init_bitmap();
void init_active_cache();
void init_double_buffer();
void init_active_store();

void close_restore_active();
void close_active();
void close_active_cache();
void close_double_buffer();
void close_bitmap();
void close_active_store();

void* active_cache_lookup(struct lruCache *c, void *user_data);
void active_kicks(struct lruCache *c, struct bitmap *b);
void active_cache_insert(struct lruCache *c, void* data);

void active_filter(struct segment *s);
struct cache_entry* active_area_lookup(struct doubleBuffer *t, fingerprint *fp);
struct chunk* read_active_chunk(struct cache_entry *victim);
void *double_filter_lookup(struct doubleBuffer *t, fingerprint *fp);
void active_filter_insert(struct doubleBuffer *t, struct chunk *ck);
void active_filter_kicks(struct doubleBuffer *t);


void start_active_dedup_phase();
void stop_active_dedup_phase();
void *active_dedup_thread(void *arg);
void send_active_segment(struct segment* s);

// active container
containerid active_insert(struct chunk *c);
void active_remove(struct doubleBuffer *t);
gint g_active_container_id_cmp(gconstpointer cid1,
		gconstpointer cid2);
struct container* create_active_container();
struct container* retrieve_active_container_by_id(containerid id);
struct containerMeta* retrieve_active_container_meta_by_id(containerid id);
struct container* retrieve_active_pool_by_id(containerid id);
void write_active_container_async(struct container *c);
void write_active_container(struct container* c);
void write_active_pool(struct container *c);

// read cons
void read_cons();
void close_cons();
void add_cons(struct container* c);
void update_cons(struct container* c, struct chunk* ck);
void merge_cons(GHashTable *t2);


// the active_bv in backup versions
struct backupVersion* create_active_backup_version(const char *path);
void start_active_recipe_phase();
void stop_active_recipe_phase();
void *active_recipe_thread(void *arg);


// the active_jcr && active_bv in restore versions
void init_active_backupVersion(char* path);
struct backupVersion* open_active_backup_version(int number);
int active_backup_version_exists(int number);
void init_restore_active_jcr(int revision, char *path);
void init_active_jcr(char *path);


// optimize the phase of restoring chunks
void start_lru_read_chunk_phase();
void stop_lru_read_chunk_phase();


// update recipe
//struct chunkPointer* active_read_next_chunk_pointers(struct recipeinfo *ri);


struct doubleBuffer* ac_table;
struct jcr active_jcr;
struct backupVersion* active_bv;

SyncQueue* active_dedup_queue;
SyncQueue* active_recipe_queue;

#endif