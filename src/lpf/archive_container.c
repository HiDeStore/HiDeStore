/*
 * create on: Oct 16, 2019
 *	  Auther: Pengfei Li
 */

#include "archive_container.h"
#include "../utils/serial.h"
#include "../storage/containerstore.h"
#include "../utils/sync_queue.h"
#include "../jcr.h"
#include "../utils/lru_cache.h"


#define ARCHIVE_CONTAINER_SIZE (4194304ll) //4MB
#define ARCHIVE_CONTAINER_META_SIZE (32768ll) //32KB

static FILE* ar_fp;
static int64_t archive_container_count = 1;
static SyncQueue* archive_container_buffer;
static pthread_t archive_t;
static pthread_mutex_t mutex;

struct metaEntry {
	int32_t off;
	int32_t len;
	fingerprint fp;
};

static void* archive_thread(void *arg) {
	while (1) {
		struct container *c = sync_queue_get_top(archive_container_buffer);
		if (c == NULL)
			break;

		write_archive_container(c);


		sync_queue_pop(archive_container_buffer);

		free_container(c);
	}

	return NULL;
}

void init_archive_store() {
	sds archivefile = sdsdup(destor.working_directory);
	archivefile = sdscat(archivefile, "active/archive.pool");

	if ((ar_fp = fopen(archivefile, "r+"))) {
		fread(&archive_container_count, 8, 1, ar_fp);
	} else if (!(ar_fp = fopen(archivefile, "w+"))) {
		perror(
				"Can not create archive.pool for read and write because");
		exit(1);
	}

	sdsfree(archivefile);
	
	pthread_mutex_init(&mutex, NULL);
	archive_container_buffer = sync_queue_new(25);
	pthread_create(&archive_t, NULL, archive_thread, NULL);

	NOTICE("Init archive_container_store, archive_container_count: %ld", archive_container_count);
}

void close_archive_store() {
	sync_queue_term(archive_container_buffer);
	pthread_join(archive_t, NULL);

	NOTICE("appending archive container successfully, current_count: %ld", archive_container_count);

	fseek(ar_fp, 0, SEEK_SET);
	fwrite(&archive_container_count, sizeof(archive_container_count), 1, ar_fp);
	fclose(ar_fp);
	ar_fp=NULL;
}

static void init_archive_container_meta(struct containerMeta *meta) {
	meta->chunk_num = 0;
	meta->data_size = 0;
	meta->id = TEMPORARY_ID;
	meta->map = g_hash_table_new_full(g_int_hash, g_fingerprint_equal, NULL,
			free);
}

struct container* create_archive_container() {
	struct container *c = (struct container*) malloc(sizeof(struct container));
	if (destor.simulation_level < SIMULATION_APPEND)
		c->data = calloc(1, CONTAINER_SIZE);
	else
		c->data = 0;

	init_archive_container_meta(&c->meta);
	c->meta.id = archive_container_count++;
	return c;
}

void write_archive_container_async(struct container *c) {
	assert(c->meta.chunk_num == g_hash_table_size(c->meta.map));

	if (container_empty(c)) {
		/* An empty container
		 * It possibly occurs in the end of backup */
		archive_container_count--;
		VERBOSE("Archive_append phase: Deny writing an empty container %lld",
				c->meta.id);
		return;
	}

	sync_queue_push(archive_container_buffer, c);
}

void write_archive_container(struct container* c) {

	assert(c->meta.chunk_num == g_hash_table_size(c->meta.map));

	if (container_empty(c)) {
		/* An empty container
		 * It possibly occurs in the end of backup */
		archive_container_count--;
		VERBOSE("Archive_append phase: Deny writing an empty container %lld",
				c->meta.id);
		return;
	}

	VERBOSE("Archive_append phase: Writing container %lld of %d chunks", c->meta.id,
			c->meta.chunk_num);

	if (destor.simulation_level < SIMULATION_APPEND) {

		unsigned char * cur = &c->data[CONTAINER_SIZE - CONTAINER_META_SIZE];
		ser_declare;
		ser_begin(cur, CONTAINER_META_SIZE);
		ser_int64(c->meta.id);
		ser_int32(c->meta.chunk_num);
		ser_int32(c->meta.data_size);

		GHashTableIter iter;
		gpointer key, value;
		g_hash_table_iter_init(&iter, c->meta.map);
		while (g_hash_table_iter_next(&iter, &key, &value)) {
			struct metaEntry *me = (struct metaEntry *) value;
			ser_bytes(&me->fp, sizeof(fingerprint));
			ser_bytes(&me->len, sizeof(int32_t));
			ser_bytes(&me->off, sizeof(int32_t));
		}

		ser_end(cur, CONTAINER_META_SIZE);

		pthread_mutex_lock(&mutex);

		if (fseek(ar_fp, c->meta.id * CONTAINER_SIZE + 8, SEEK_SET) != 0) {
			perror("Fail seek in container store.");
			exit(1);
		}
		if(fwrite(c->data, CONTAINER_SIZE, 1, ar_fp) != 1){
			perror("Fail to write a container in container store.");
			exit(1);
		}

		pthread_mutex_unlock(&mutex);
	} else {
		char buf[CONTAINER_META_SIZE];
		memset(buf, 0, CONTAINER_META_SIZE);

		ser_declare;
		ser_begin(buf, CONTAINER_META_SIZE);
		ser_int64(c->meta.id);
		ser_int32(c->meta.chunk_num);
		ser_int32(c->meta.data_size);

		GHashTableIter iter;
		gpointer key, value;
		g_hash_table_iter_init(&iter, c->meta.map);
		while (g_hash_table_iter_next(&iter, &key, &value)) {
			struct metaEntry *me = (struct metaEntry *) value;
			ser_bytes(&me->fp, sizeof(fingerprint));
			ser_bytes(&me->len, sizeof(int32_t));
			ser_bytes(&me->off, sizeof(int32_t));
		}

		ser_end(buf, CONTAINER_META_SIZE);

		pthread_mutex_lock(&mutex);

		if(fseek(ar_fp, c->meta.id * CONTAINER_META_SIZE + 8, SEEK_SET) != 0){
			perror("Fail seek in container store.");
			exit(1);
		}
		if(fwrite(buf, CONTAINER_META_SIZE, 1, ar_fp) != 1){
			perror("Fail to write a container in container store.");
			exit(1);
		}

		pthread_mutex_unlock(&mutex);
	}

}

static void* container_meta_duplicate(void *tc) {
	struct container *c = (struct container *)tc;
	struct containerMeta* base = &c->meta;
	struct containerMeta* dup = (struct containerMeta*) malloc(
			sizeof(struct containerMeta));
	init_archive_container_meta(dup);
	dup->id = base->id;
	dup->chunk_num = base->chunk_num;
	dup->data_size = base->data_size;

	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, base->map);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		struct metaEntry* me = (struct metaEntry*) malloc(
				sizeof(struct metaEntry));
		memcpy(me, value, sizeof(struct metaEntry));
		g_hash_table_insert(dup->map, &me->fp, me);
	}

	return dup;
}

struct containerMeta* retrieve_archive_container_meta_by_id(containerid id) {
	struct containerMeta* cm = NULL;

	/* First, we find it in the buffer */
	cm = sync_queue_find(archive_container_buffer, container_check_id, &id,
			container_meta_duplicate);

	if (cm)
		return cm;

	cm = (struct containerMeta*) malloc(sizeof(struct containerMeta));
	init_archive_container_meta(cm);

	unsigned char buf[CONTAINER_META_SIZE];

	pthread_mutex_lock(&mutex);

	if (destor.simulation_level >= SIMULATION_APPEND)
		fseek(ar_fp, id * CONTAINER_META_SIZE + 8, SEEK_SET);
	else
		fseek(ar_fp, (id + 1) * CONTAINER_SIZE - CONTAINER_META_SIZE + 8,
		SEEK_SET);

	fread(buf, CONTAINER_META_SIZE, 1, ar_fp);

	pthread_mutex_unlock(&mutex);

	unser_declare;
	unser_begin(buf, CONTAINER_META_SIZE);

	unser_int64(cm->id);
	unser_int32(cm->chunk_num);
	unser_int32(cm->data_size);

	if(cm->id != id){
		WARNING("expect %lld, but read %lld", id, cm->id);
		assert(cm->id == id);
	}

	int i;
	for (i = 0; i < cm->chunk_num; i++) {
		struct metaEntry* me = (struct metaEntry*) malloc(
				sizeof(struct metaEntry));
		unser_bytes(&me->fp, sizeof(fingerprint));
		unser_bytes(&me->len, sizeof(int32_t));
		unser_bytes(&me->off, sizeof(int32_t));
		g_hash_table_insert(cm->map, &me->fp, me);
	}

	return cm;
}


struct lruCache *create_archive_restore_cache() {
	struct lruCache *ar_cache = new_lru_cache(-1, free_container_meta,
				lookup_fingerprint_in_container_meta);
	int i= 0;
	for(i=0; i<archive_container_count; i++) {
		struct containerMeta *cm = retrieve_archive_container_meta_by_id(i);
		lru_cache_insert(ar_cache, cm, NULL, NULL);
	}

	printf("create_archive_container: %d\n", ar_cache->size);
	return ar_cache;
}

struct lruCache *create_archive_container_cache() {
	struct lruCache *ar_cache = new_lru_cache(-1, free_container,
				lookup_fingerprint_in_container);
	int i= 0;
	for(i=0; i<archive_container_count; i++) {
		struct container *con = retrieve_archive_container_by_id(i);
		lru_cache_insert(ar_cache, con, NULL, NULL);
	}

	printf("create_archive_container: %d\n", ar_cache->size);
	return ar_cache;
}


struct container* retrieve_archive_container_by_id(containerid id) {
	struct container *c = (struct container*) malloc(sizeof(struct container));

	init_archive_container_meta(&c->meta);

	unsigned char *cur = 0;
	if (destor.simulation_level >= SIMULATION_RESTORE) {
		c->data = malloc(CONTAINER_META_SIZE);

		pthread_mutex_lock(&mutex);

		if (destor.simulation_level >= SIMULATION_APPEND)
			fseek(ar_fp, id * CONTAINER_META_SIZE + 8, SEEK_SET);
		else
			fseek(ar_fp, (id + 1) * CONTAINER_SIZE - CONTAINER_META_SIZE + 8,
			SEEK_SET);

		fread(c->data, CONTAINER_META_SIZE, 1, ar_fp);

		pthread_mutex_unlock(&mutex);

		cur = c->data;
	} else {
		c->data = malloc(CONTAINER_SIZE);

		pthread_mutex_lock(&mutex);

		fseek(ar_fp, id * CONTAINER_SIZE + 8, SEEK_SET);
		fread(c->data, CONTAINER_SIZE, 1, ar_fp);

		pthread_mutex_unlock(&mutex);

		cur = &c->data[CONTAINER_SIZE - CONTAINER_META_SIZE];
	}

	unser_declare;
	unser_begin(cur, CONTAINER_META_SIZE);

	unser_int64(c->meta.id);
	unser_int32(c->meta.chunk_num);
	unser_int32(c->meta.data_size);

	if(c->meta.id != id){
		WARNING("expect %lld, but read %lld", id, c->meta.id);
		assert(c->meta.id == id);
	}

	int i;
	for (i = 0; i < c->meta.chunk_num; i++) {
		struct metaEntry* me = (struct metaEntry*) malloc(
				sizeof(struct metaEntry));
		unser_bytes(&me->fp, sizeof(fingerprint));
		unser_bytes(&me->len, sizeof(int32_t));
		unser_bytes(&me->off, sizeof(int32_t));
		g_hash_table_insert(c->meta.map, &me->fp, me);
	}

	unser_end(cur, CONTAINER_META_SIZE);

	if (destor.simulation_level >= SIMULATION_RESTORE) {
		free(c->data);
		c->data = 0;
	}

	return c;
}
