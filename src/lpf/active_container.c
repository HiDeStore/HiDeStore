/*
 * create by Pengfei Li
 *		2020.1.7
 */

#include "../utils/serial.h"
#include "../storage/containerstore.h"
#include "../utils/sync_queue.h"
#include "../jcr.h"
#include "../utils/lru_cache.h"
#include "active.h"

#define SPARSE (0.5)

static FILE *ac_fp;
static int64_t active_container_count = 0;
static SyncQueue* active_container_buffer;
static pthread_t active_t;
static pthread_mutex_t mutex;
static sds activepoolpath;


struct {
	GHashTable *normal_cons;
	GHashTable *sparse_cons;
	GHashTable *ideal_cons;
} cons;


struct conMeta {
	containerid id;
	int32_t data_size;
	int32_t chunk_num;
};


struct metaEntry {
	int32_t off;
	int32_t len;
	fingerprint fp;
};

static void init_active_container_meta(struct containerMeta *meta) {
	meta->chunk_num = 0;
	meta->data_size = 0;
	meta->id = TEMPORARY_ID;
	meta->map = g_hash_table_new_full(g_int_hash, g_fingerprint_equal, NULL,
			free);
}

static void* active_thread(void *arg) {
	while (1) {
		struct container *c = sync_queue_get_top(active_container_buffer);
		if (c == NULL)
			break;

		write_active_container(c);
		//write_active_pool(c);


		sync_queue_pop(active_container_buffer);

		free_container(c);
	}

	return NULL;
}

void init_active_store(){
	activepoolpath = sdsdup(destor.working_directory);
	activepoolpath = sdscat(activepoolpath, "active/activePool/con");

	sds activefile = sdsdup(destor.working_directory);
	activefile = sdscat(activefile, "active/active.pool");
	if ((ac_fp = fopen(activefile, "r+"))) {
		fread(&active_container_count, 8, 1, ac_fp);
	} else if (!(ac_fp = fopen(activefile, "w+"))) {
		perror(
				"Can not create active.pool for read and write because");
		exit(1);
	}
	sdsfree(activefile);
	NOTICE("Init active_container_count: %ld", active_container_count);
	read_cons();

	pthread_mutex_init(&mutex, NULL);
	active_container_buffer = sync_queue_new(25);
	pthread_create(&active_t, NULL, active_thread, NULL);
}


void close_active_store(){
	sync_queue_term(active_container_buffer);
	pthread_join(active_t, NULL);
	NOTICE("appending active container successfully, current_count: %ld", active_container_count);
	fseek(ac_fp, 0, SEEK_SET);
	fwrite(&active_container_count, sizeof(active_container_count), 1, ac_fp);
	fclose(ac_fp);
	ac_fp=NULL;
	sdsfree(activepoolpath);

	close_cons();
}



void write_active_container(struct container* c) {

	assert(c->meta.chunk_num == g_hash_table_size(c->meta.map));

	if (container_empty(c)) {
		/* An empty container
		 * It possibly occurs in the end of backup */
		if(c->meta.id < active_container_count-1){
			struct conMeta *cm = g_hash_table_lookup(cons.normal_cons, &c->meta.id);
			if(cm){
				g_hash_table_remove(cons.normal_cons, &c->meta.id);
			}
			cm = (struct conMeta *)malloc(sizeof(struct conMeta));
			cm->id = c->meta.id;
			cm->data_size = 0;
			cm->chunk_num=0;
			g_hash_table_insert(cons.ideal_cons, &cm->id, cm);

			NOTICE("Generate ideal active container: %ld", cm->id);
		} else {
			active_container_count--;
		}
		VERBOSE("Active_append phase: Deny writing an empty container %lld",
				c->meta.id);
		return;
	}

	VERBOSE("Active_append phase: Writing container %lld of %d chunks", c->meta.id,
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

		if (fseek(ac_fp, c->meta.id * CONTAINER_SIZE + 8, SEEK_SET) != 0) {
			perror("Fail seek in container store.");
			exit(1);
		}
		if(fwrite(c->data, CONTAINER_SIZE, 1, ac_fp) != 1){
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

		if(fseek(ac_fp, c->meta.id * CONTAINER_META_SIZE + 8, SEEK_SET) != 0){
			perror("Fail seek in container store.");
			exit(1);
		}
		if(fwrite(buf, CONTAINER_META_SIZE, 1, ac_fp) != 1){
			perror("Fail to write a container in container store.");
			exit(1);
		}

		pthread_mutex_unlock(&mutex);
	}

	add_cons(c);

}

void write_active_container_async(struct container *c) {
	assert(c->meta.chunk_num == g_hash_table_size(c->meta.map));

	if (container_empty(c)) {
		/* An empty container
		 * It possibly occurs in the end of backup */
		if(c->meta.id < active_container_count-1){
			struct conMeta *cm = g_hash_table_lookup(cons.normal_cons, &c->meta.id);
			if(cm){
				g_hash_table_remove(cons.normal_cons, &c->meta.id);
			}
			cm = (struct conMeta *)malloc(sizeof(struct conMeta));
			cm->id = c->meta.id;
			cm->data_size = 0;
			cm->chunk_num=0;
			g_hash_table_insert(cons.ideal_cons, &cm->id, cm);

			NOTICE("Generate ideal active container: %ld", cm->id);
		} else {
			active_container_count--;
		}
		VERBOSE("Active_append phase: Deny writing an empty container %lld",
				c->meta.id);
		return;
	}

	sync_queue_push(active_container_buffer, c);
}


gint g_active_container_id_cmp(gconstpointer cid1,
		gconstpointer cid2) {
	return *((containerid*)cid1)-*((containerid*)cid2);
}


struct container* create_active_container() {
	struct container *c = (struct container*) malloc(sizeof(struct container));
	if (destor.simulation_level < SIMULATION_APPEND)
		c->data = calloc(1, CONTAINER_SIZE);
	else
		c->data = 0;

	init_active_container_meta(&c->meta);
	if(g_hash_table_size(cons.ideal_cons) > 0){
		GList *ids = g_hash_table_get_keys(cons.ideal_cons);
		ids=g_list_sort(ids, g_active_container_id_cmp);
		struct conMeta* cm = g_hash_table_lookup(cons.ideal_cons, g_list_first(ids)->data);
		c->meta.id = cm->id;
		g_hash_table_remove(cons.ideal_cons, &cm->id);
	} else {
		c->meta.id = active_container_count++;
	}
	
	NOTICE("Create active container: %ld", c->meta.id);
	return c;
}

struct container* retrieve_active_container_by_id(containerid id) {
	struct container *c = (struct container*) malloc(sizeof(struct container));

	init_active_container_meta(&c->meta);

	unsigned char *cur = 0;
	if (destor.simulation_level >= SIMULATION_RESTORE) {
		c->data = malloc(CONTAINER_META_SIZE);

		pthread_mutex_lock(&mutex);

		if (destor.simulation_level >= SIMULATION_APPEND)
			fseek(ac_fp, id * CONTAINER_META_SIZE + 8, SEEK_SET);
		else
			fseek(ac_fp, (id + 1) * CONTAINER_SIZE - CONTAINER_META_SIZE + 8,
			SEEK_SET);

		fread(c->data, CONTAINER_META_SIZE, 1, ac_fp);

		pthread_mutex_unlock(&mutex);

		cur = c->data;
	} else {
		c->data = malloc(CONTAINER_SIZE);

		pthread_mutex_lock(&mutex);

		fseek(ac_fp, id * CONTAINER_SIZE + 8, SEEK_SET);
		fread(c->data, CONTAINER_SIZE, 1, ac_fp);

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


static void* container_meta_duplicate(void *c) {
	struct containerMeta* base = &((struct container *)c)->meta;
	struct containerMeta* dup = (struct containerMeta*) malloc(
			sizeof(struct containerMeta));
	init_active_container_meta(dup);
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

	return (void *)dup;
}

struct containerMeta* retrieve_active_container_meta_by_id(containerid id) {
	struct containerMeta* cm = NULL;

	/* First, we find it in the buffer */
	cm = sync_queue_find(active_container_buffer, container_check_id, &id,
			container_meta_duplicate);

	if (cm)
		return cm;

	cm = (struct containerMeta*) malloc(sizeof(struct containerMeta));
	init_active_container_meta(cm);

	unsigned char buf[CONTAINER_META_SIZE];

	pthread_mutex_lock(&mutex);

	if (destor.simulation_level >= SIMULATION_APPEND)
		fseek(ac_fp, id * CONTAINER_META_SIZE + 8, SEEK_SET);
	else
		fseek(ac_fp, (id + 1) * CONTAINER_SIZE - CONTAINER_META_SIZE + 8,
		SEEK_SET);

	fread(buf, CONTAINER_META_SIZE, 1, ac_fp);

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


// ============================= the table for containers =======================
void read_cons(){
	sds activefile = sdsdup(destor.working_directory);
	activefile = sdscat(activefile, "active/activeCons");
	
	cons.normal_cons = g_hash_table_new(g_int64_hash, g_int64_equal);
	cons.sparse_cons = g_hash_table_new(g_int64_hash, g_int64_equal);
	cons.ideal_cons = g_hash_table_new(g_int64_hash, g_int64_equal);
	
	FILE *fp;
	if ((fp=fopen(activefile, "r"))) {
		
		int key_num;
		fread(&key_num, sizeof(int), 1, fp);
		for(; key_num>0; key_num--){
			struct conMeta *cm = (struct conMeta *)malloc(sizeof(struct conMeta));
			fread(&cm->id, sizeof(containerid), 1, fp);
			fread(&cm->data_size, sizeof(int32_t), 1, fp);
			fread(&cm->chunk_num, sizeof(int32_t), 1, fp);

			g_hash_table_insert(cons.normal_cons, &cm->id, cm);
		}
		//sparse containers
		key_num;
		fread(&key_num, sizeof(int), 1, fp);
		for(; key_num>0; key_num--){
			struct conMeta *cm = (struct conMeta *)malloc(sizeof(struct conMeta));
			fread(&cm->id, sizeof(containerid), 1, fp);
			fread(&cm->data_size, sizeof(int32_t), 1, fp);
			fread(&cm->chunk_num, sizeof(int32_t), 1, fp);

			g_hash_table_insert(cons.sparse_cons, &cm->id, cm);
		}
		// ideal containers
		key_num;
		fread(&key_num, sizeof(int), 1, fp);
		for(; key_num>0; key_num--){
			struct conMeta *cm = (struct conMeta *)malloc(sizeof(struct conMeta));
			fread(&cm->id, sizeof(containerid), 1, fp);
			fread(&cm->data_size, sizeof(int32_t), 1, fp);
			fread(&cm->chunk_num, sizeof(int32_t), 1, fp);

			g_hash_table_insert(cons.ideal_cons, &cm->id, cm);
		}
		fclose(fp);
	}
	sdsfree(activefile);

	NOTICE("Read normal, sparse, ideal containers: %d, %d, %d", g_hash_table_size(cons.normal_cons),
		g_hash_table_size(cons.sparse_cons), g_hash_table_size(cons.ideal_cons));
}

void close_cons(){
	sds activefile = sdsdup(destor.working_directory);
	activefile = sdscat(activefile, "active/activeCons");

	FILE *fp;
	if ((fp=fopen(activefile, "w")) == NULL) {
		perror("Can not open active/activeCons for write because:");
		exit(1);
	}

	NOTICE("flushing normal, sparse, ideal containers: %d, %d, %d", g_hash_table_size(cons.normal_cons),
		g_hash_table_size(cons.sparse_cons), g_hash_table_size(cons.ideal_cons));

	int key_num = g_hash_table_size(cons.normal_cons);
	if(fwrite(&key_num, sizeof(int), 1, fp) != 1){
		perror("Fail to write a key_num!");
		exit(1);
	}

	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, cons.normal_cons);
	while(g_hash_table_iter_next(&iter, &key, &value)){
		struct conMeta *cm = (struct conMeta *)value;
		if(fwrite(&cm->id, sizeof(int64_t), 1, fp) != 1){
			perror("Fail to write an id!");
			exit(1);
		}

		if(fwrite(&cm->data_size, sizeof(int32_t), 1, fp) != 1) {
			perror("Fail to write a data_size!");
			exit(1);
		}

		if(fwrite(&cm->chunk_num, sizeof(int32_t), 1, fp) != 1) {
			perror("Fail to write a chunk_num!");
			exit(1);
		}
	}

	// sparse containers
	key_num = g_hash_table_size(cons.sparse_cons);
	if(fwrite(&key_num, sizeof(int), 1, fp) != 1){
		perror("Fail to write a key_num!");
		exit(1);
	}
	g_hash_table_iter_init(&iter, cons.sparse_cons);
	while(g_hash_table_iter_next(&iter, &key, &value)){
		struct conMeta *cm = (struct conMeta *)value;
		if(fwrite(&cm->id, sizeof(int64_t), 1, fp) != 1){
			perror("Fail to write an id!");
			exit(1);
		}
		if(fwrite(&cm->data_size, sizeof(int32_t), 1, fp) != 1) {
			perror("Fail to write a data_size!");
			exit(1);
		}
		if(fwrite(&cm->chunk_num, sizeof(int32_t), 1, fp) != 1) {
			perror("Fail to write a chunk_num!");
			exit(1);
		}
	}

	// ideal containers
	key_num = g_hash_table_size(cons.ideal_cons);
	if(fwrite(&key_num, sizeof(int), 1, fp) != 1){
		perror("Fail to write a key_num!");
		exit(1);
	}
	g_hash_table_iter_init(&iter, cons.ideal_cons);
	while(g_hash_table_iter_next(&iter, &key, &value)){
		struct conMeta *cm = (struct conMeta *)value;
		if(fwrite(&cm->id, sizeof(int64_t), 1, fp) != 1){
			perror("Fail to write an id!");
			exit(1);
		}
		if(fwrite(&cm->data_size, sizeof(int32_t), 1, fp) != 1) {
			perror("Fail to write a data_size!");
			exit(1);
		}
		if(fwrite(&cm->chunk_num, sizeof(int32_t), 1, fp) != 1) {
			perror("Fail to write a chunk_num!");
			exit(1);
		}
	}
		
	fclose(fp);

	sdsfree(activefile);
}

void add_cons(struct container* c){
	struct conMeta *cm = g_hash_table_lookup(cons.normal_cons, &c->meta.id);
	assert(!cm);
	cm = (struct conMeta *)malloc(sizeof(struct conMeta));
	cm->id = c->meta.id;
	cm->data_size = c->meta.data_size;
	cm->chunk_num=c->meta.chunk_num;

	if( (c->meta.data_size/(float)(CONTAINER_SIZE - CONTAINER_META_SIZE) < SPARSE) 
		&& ((c->meta.chunk_num*28+16)/(float)CONTAINER_META_SIZE<SPARSE)){
		NOTICE("Generate sparse_container: %ld", cm->id);
		struct conMeta *spr = g_hash_table_lookup(cons.sparse_cons, &c->meta.id);
		assert(!spr);
		g_hash_table_insert(cons.sparse_cons, &cm->id, cm);
		return;
	}

	g_hash_table_insert(cons.normal_cons, &cm->id, cm);
}

void update_cons(struct container* c, struct chunk* ck)
{
	struct conMeta *cm = g_hash_table_lookup(cons.normal_cons, &c->meta.id);
	if(cm){
		cm->data_size -= ck->size;
		cm->chunk_num--;
		if( (cm->data_size/(float)(CONTAINER_SIZE - CONTAINER_META_SIZE) < SPARSE) 
			&& ((cm->chunk_num*28+16)/(float)CONTAINER_META_SIZE<SPARSE)){
			g_hash_table_steal(cons.normal_cons, &cm->id);
			g_hash_table_insert(cons.sparse_cons, &cm->id, cm);
			NOTICE("Generate sparse_container: %ld", cm->id);
		}

		return;
	}

	cm = g_hash_table_lookup(cons.sparse_cons, &c->meta.id);
	assert(cm);
	cm->data_size -= ck->size;
	cm->chunk_num--;
	if(cm->chunk_num == 0){
		g_hash_table_steal(cons.sparse_cons, &cm->id);
		g_hash_table_insert(cons.ideal_cons, &cm->id, cm);
		NOTICE("Generate ideal_container: %ld", cm->id);
	}
}

void merge_cons(GHashTable *t2){	
	while(g_hash_table_size(cons.sparse_cons) >= 1/SPARSE){

		// get 1/SPARSE sparse containers
		GList *ids = g_hash_table_get_keys(cons.sparse_cons);
		ids=g_list_sort(ids, g_active_container_id_cmp);
		GList *next = g_list_first(ids);
		struct conMeta* merge[(int)(1/SPARSE)];
		int i=0;
		for(i=0; i<1/SPARSE; i++){
			merge[i] = g_hash_table_lookup(cons.sparse_cons, next->data);
			next = next->next;
		}

		// create a new active container to merge the sparse containers, id=smallest sparse cid
		struct container *nc = (struct container*) malloc(sizeof(struct container));
		if (destor.simulation_level < SIMULATION_APPEND)
			nc->data = calloc(1, CONTAINER_SIZE);
		else
			nc->data = 0;
		init_active_container_meta(&nc->meta);
		nc->meta.id = merge[0]->id;

		
		GHashTableIter iter;
		gpointer key, value;
		// merge the first container
		struct container* con = retrieve_active_container_by_id(merge[0]->id);
		g_hash_table_iter_init(&iter, con->meta.map);
		
		while(g_hash_table_iter_next(&iter, &key, &value)){
			struct metaEntry *me = (struct metaEntry *) value;
			struct cache_entry *ce = g_hash_table_lookup(t2,  &me->fp);
			if(ce){ 	
				struct chunk *ck = get_chunk_in_container(con, &me->fp);
				memcpy(&ck->fp, &me->fp, sizeof(fingerprint));
				/*SHA_CTX ctx;
				SHA_Init(&ctx);
				SHA_Update(&ctx, ck->data, ck->size);
				SHA_Final(ck->fp, &ctx);
				assert(memcmp(&ck->fp, &me->fp, sizeof(fingerprint))==0);*/
				if(add_chunk_to_container(nc, ck)){
					ce->id = ck->id;
				}
			}
		}
		g_hash_table_remove(cons.sparse_cons, &merge[0]->id);
		
		// merge the other containers
		for(i=1; i<1/SPARSE; i++){
			con = retrieve_active_container_by_id(merge[i]->id);
			g_hash_table_iter_init(&iter, con->meta.map);

			while(g_hash_table_iter_next(&iter, &key, &value)){
				struct metaEntry *me = (struct metaEntry *) value;
				struct cache_entry *ce = g_hash_table_lookup(t2,  &me->fp);
				if(ce){
					struct chunk *ck = get_chunk_in_container(con, &me->fp);
					memcpy(&ck->fp, &me->fp, sizeof(fingerprint));
					/*SHA_CTX ctx;
					SHA_Init(&ctx);
					SHA_Update(&ctx, ck->data, ck->size);
					SHA_Final(ck->fp, &ctx);
					assert(memcmp(&ck->fp, &me->fp, sizeof(fingerprint))==0);*/
					if(add_chunk_to_container(nc, ck)){
						ce->id = ck->id;
					}
				}
			}
			merge[i]->data_size = 0;
			merge[i]->chunk_num = 0;
			g_hash_table_steal(cons.sparse_cons, &merge[i]->id);
			g_hash_table_insert(cons.ideal_cons, &merge[i]->id, merge[i]);
			NOTICE("Generate ideal_container: %ld", merge[i]->id);
		}
		// write nc
		write_active_container(nc);
		//write_active_pool(nc);
		NOTICE("Merge to active container: %ld", nc->meta.id);
	}
}


// ======================== write and read active_pools ====================
// ================ used to replace the write_active_container() function ==============
void write_active_pool(struct container *c){
	
	assert(c->meta.chunk_num == g_hash_table_size(c->meta.map));

	if (container_empty(c)) {
		/* An empty container
		 * It possibly occurs in the end of backup */
		if(c->meta.id < active_container_count-1){
			struct conMeta *cm = g_hash_table_lookup(cons.normal_cons, &c->meta.id);
			if(cm){
				g_hash_table_remove(cons.normal_cons, &c->meta.id);
			}
			cm = (struct conMeta *)malloc(sizeof(struct conMeta));
			cm->id = c->meta.id;
			cm->data_size = 0;
			cm->chunk_num=0;
			g_hash_table_insert(cons.ideal_cons, &cm->id, cm);

			NOTICE("Generate ideal active container: %ld", cm->id);
		} else {
			active_container_count--;
		}
		VERBOSE("Active_append phase: Deny writing an empty container %lld",
				c->meta.id);
		return;
	}

	sds confile = sdsdup(activepoolpath);
	char s[20];
	sprintf(s, "%d", c->meta.id);
	confile = sdscat(confile, s);
	FILE *fp;
	if (!(fp = fopen(confile, "w"))) {
		perror("Can not create active/activePool for read and write because");
		exit(1);
	}

	NOTICE("Active_append phase: Writing container %lld of %d chunks", c->meta.id,
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

		if (fseek(fp, c->meta.id * CONTAINER_SIZE, SEEK_SET) != 0) {
			perror("Fail seek in container store.");
			exit(1);
		}
		if(fwrite(c->data, CONTAINER_SIZE, 1, fp) != 1){
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

		if(fseek(fp, c->meta.id * CONTAINER_META_SIZE, SEEK_SET) != 0){
			perror("Fail seek in container store.");
			exit(1);
		}
		if(fwrite(buf, CONTAINER_META_SIZE, 1, fp) != 1){
			perror("Fail to write a container in container store.");
			exit(1);
		}

		pthread_mutex_unlock(&mutex);
	}

	add_cons(c);
	sdsfree(confile);
	fclose(fp);
}


struct container* retrieve_active_pool_by_id(containerid id) {
	sds confile = sdsdup(activepoolpath);
	char s[20];
	sprintf(s, "%d", id);
	confile = sdscat(confile, s);
	FILE *fp;
	if (!(fp = fopen(confile, "r"))) {
		perror("Can not read active/activePool for read and write because");
		exit(1);
	}

	struct container *c = (struct container*) malloc(sizeof(struct container));

	init_active_container_meta(&c->meta);

	unsigned char *cur = 0;
	if (destor.simulation_level >= SIMULATION_RESTORE) {
		c->data = malloc(CONTAINER_META_SIZE);

		pthread_mutex_lock(&mutex);

		if (destor.simulation_level >= SIMULATION_APPEND)
			fseek(fp, id * CONTAINER_META_SIZE, SEEK_SET);
		else
			fseek(fp, (id + 1) * CONTAINER_SIZE - CONTAINER_META_SIZE,
			SEEK_SET);

		fread(c->data, CONTAINER_META_SIZE, 1, fp);

		pthread_mutex_unlock(&mutex);

		cur = c->data;
	} else {
		c->data = malloc(CONTAINER_SIZE);

		pthread_mutex_lock(&mutex);

		fseek(fp, id * CONTAINER_SIZE, SEEK_SET);
		fread(c->data, CONTAINER_SIZE, 1, fp);

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

	fclose(fp);
	sdsfree(confile);

	return c;
}


