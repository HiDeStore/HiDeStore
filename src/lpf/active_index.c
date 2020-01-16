/*
 * create on: Oct 11, 2019
 *	  Auther: Pengfei Li
 */

#include "../destor.h"
#include "../jcr.h"
#include "archive_container.h"
#include "../storage/containerstore.h"
#include "../recipe/recipestore.h"
#include "active.h"
#include <math.h>
#include <time.h>
#include "../index/index.h"
#include "../index/kvstore.h"
#include "../index/fingerprint_cache.h"
#include "../index/index_buffer.h"
#include "../utils/serial.h"
#include "../utils/sync_queue.h"


#define STATISTIC_NUM (10001)
#define ACTIVE_SIZE (67108864ll)  // 64M
//#define ACTIVE_SIZE (1073741824ll)  	// 1G
#define PAGE_SIZE (1024)          	// 1K



GHashTable *ac_cache;
struct bitmap* b;
struct container *archive_container;
struct container *active_container;


struct {
	int64_t data_size;
	int64_t dedup_size;
	int64_t stored_size;

	double dedup_time;
} ac_record;


struct doubleBuffer *new_double_buffer(){
	struct doubleBuffer *db = (struct doubleBuffer *)malloc(sizeof(struct doubleBuffer));
	db->t1 = g_hash_table_new_full(g_int_hash, g_fingerprint_equal, NULL, free);
	db->t2 = g_hash_table_new_full(g_int_hash, g_fingerprint_equal, NULL, free);
	db->t1size = 0;
	db->t2size = 0;

	return db;
}

void free_entry(void* ce){
	free((struct cache_entry*)ce);
}

int cache_entry_equal(void *ce, void *fp) {
	return memcmp( &(((struct cache_entry *)ce)->fp), (fingerprint *)fp, sizeof(fingerprint)) == 0? 1:0 ;
}

void init_active() {

	//init_bitmap();
	init_active_cache();
	init_double_buffer();
	
	init_active_store();
	init_archive_store();
	archive_container = NULL;

	ac_record.data_size = 0;
	ac_record.dedup_size = 0;
	ac_record.stored_size = 0;
	ac_record.dedup_time = 0.0;
}


void init_double_buffer() {
	sds doublebufferpath = sdsdup(destor.working_directory);
	doublebufferpath =  sdscat(doublebufferpath, "active/doublebuffer");

	ac_table = new_double_buffer();
	FILE *fp;
	if ((fp=fopen(doublebufferpath, "r"))) {
		
		int key_num;
		fread(&key_num, sizeof(int), 1, fp);
		for(; key_num>0; key_num--){
			struct cache_entry *ce = (struct cache_entry *)malloc(sizeof(struct cache_entry));
			fread(&ce->fp, destor.index_key_size, 1, fp);
			fread(&ce->flag, sizeof(int32_t), 1, fp);
			fread(&ce->size, sizeof(int32_t), 1, fp);
			fread(&ce->id, sizeof(int64_t), 1, fp);
			
			/*ce->pages = NULL;
			int required_pages = ceil(ce->size/(float)PAGE_SIZE), i;
			if (required_pages>0){
				ce->pages = (unsigned int*)malloc(required_pages*sizeof(unsigned int));
				for(i=0; i<required_pages; i++)
					fread(&(ce->pages[i]), sizeof(unsigned int), 1, fp);
			}*/

			g_hash_table_insert(ac_table->t1, &ce->fp, ce);
			ac_table->t1size++;
		}
		fclose(fp);
	}
	sdsfree(doublebufferpath);

	NOTICE("Init dobule cache buffer size: %d", ac_table->t1size);

}

void close_restore_active() {
	archive_container = NULL;
	close_archive_store();
}


void close_active() {
	if (active_container && !container_empty(active_container)) {
		write_active_container_async(active_container);
	}

	close_active_cache();
	//close_bitmap();
	//active_filter_kicks(ac_table);
	clock_t start, finish;
	double remove_time = 0.0;
	start = clock();
	//active_remove(ac_table);
	//remove_time = (double)(finish - start) / CLOCKS_PER_SEC;

	archive_container = NULL;
	close_active_store();
	close_archive_store();
	close_double_buffer();
	
	g_hash_table_destroy(ac_table->t1);
	g_hash_table_destroy(ac_table->t2);

	printf("backupversion: %d\n", jcr.id);
	printf("data_size: %" PRId64 ", dedup_size: %" PRId64 ", stored_size: %" PRId64 "\n", 
		ac_record.data_size, ac_record.dedup_size, ac_record.stored_size);
	printf("dedup ratio: %.4f\n", ac_record.data_size != 0 ?
					(ac_record.dedup_size)/ (double) (ac_record.data_size) : 0);

	printf("dedup_time : %.3fms, %.2fMB/s\n", ac_record.dedup_time / 1000,
			ac_record.data_size * 1000000 / ac_record.dedup_time / 1024 / 1024);


	destor.data_size += ac_record.data_size;
	destor.stored_data_size += ac_record.stored_size + jcr.rewritten_chunk_size;

	destor.chunk_num += jcr.chunk_num;
	destor.stored_chunk_num += jcr.unique_chunk_num + jcr.rewritten_chunk_num;
	destor.zero_chunk_num += jcr.zero_chunk_num;
	destor.zero_chunk_size += jcr.zero_chunk_size;
	destor.rewritten_chunk_num += jcr.rewritten_chunk_num;
	destor.rewritten_chunk_size += jcr.rewritten_chunk_size;

	char logfile[] = "backup.log";
	FILE *fp = fopen(logfile, "a");
	fprintf(fp, "%" PRId32 " %" PRId64 " %" PRId64 " %" PRId64 " %.4f %.3f %.2f\n",
			jcr.id,
			ac_record.data_size,
			ac_record.dedup_size,
			ac_record.stored_size,
			ac_record.data_size != 0 ?
					(ac_record.dedup_size)/ (double) (ac_record.data_size) : 0,
			ac_record.dedup_time / 1000,
			ac_record.data_size * 1000000 / ac_record.dedup_time / 1024 / 1024);

	fclose(fp);

	char staticfile[] = "static.log";
	FILE *sfp = fopen(staticfile, "a");
	fprintf(sfp, "%4d %3.4f %3.3f %7.2f %.2lf\n", 
		jcr.id,
		ac_record.data_size != 0 ?
					(ac_record.dedup_size)/ (double) (ac_record.data_size) : 0,
		ac_record.dedup_time / 1000,
		ac_record.data_size * 1000000 / ac_record.dedup_time / 1024 / 1024),
		remove_time;
	fclose(sfp);
}




void close_double_buffer() {
	sds doublebufferpath = sdsdup(destor.working_directory);
	doublebufferpath =  sdscat(doublebufferpath, "active/doublebuffer");

	FILE *fp;
	if ((fp=fopen(doublebufferpath, "w")) == NULL) {
		perror("Can not open active/cache for write because:");
		exit(1);
	}

	NOTICE("flushing double buffer: %d, %d", ac_table->t1size, ac_table->t2size);
	int key_num = ac_table->t2size;
	if(fwrite(&key_num, sizeof(int), 1, fp) != 1){
		perror("Fail to write a key_num!");
		exit(1);
	}

	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, ac_table->t2);
	while(g_hash_table_iter_next(&iter, &key, &value)){
		struct cache_entry *ce = (struct cache_entry *)value;
		if(fwrite(&ce->fp, destor.index_key_size, 1, fp) != 1){
			perror("Fail to write a key!");
			exit(1);
		}

		if(fwrite(&ce->flag, sizeof(int32_t), 1, fp) != 1) {
			perror("Fail to write a flag!");
			exit(1);
		}

		if(fwrite(&ce->size, sizeof(int32_t), 1, fp) != 1) {
			perror("Fail to write a size!");
			exit(1);
		}

		if(fwrite(&ce->id, sizeof(int64_t), 1, fp) != 1) {
			perror("Fail to write an id!");
			exit(1);
		}
		/*int required_pages = ceil(ce->size/(float)PAGE_SIZE), i;
		if(required_pages>0){
			for(i=0; i<required_pages; i++) {
				if(fwrite(&(ce->pages[i]), sizeof(unsigned int), 1, fp) != 1) {
					perror("Fail to write a page!");
					exit(1);
				}
			}
		}*/

	}
		
	fclose(fp);

	sdsfree(doublebufferpath);
}


struct cache_entry* active_area_lookup(struct doubleBuffer *t, fingerprint *fp) {
	struct cache_entry* ce = g_hash_table_lookup(t->t1, fp);
	if(ce)
		return ce;

	return g_hash_table_lookup(t->t2, fp);
}


void *double_filter_lookup(struct doubleBuffer *t, fingerprint *fp) {
	struct cache_entry* ce = g_hash_table_lookup(t->t1, fp);
	if(ce) {
		g_hash_table_steal(t->t1, fp);
		g_hash_table_insert(t->t2, &ce->fp, ce);
		t->t1size--;
		t->t2size++;

		return ce;
	}

	return g_hash_table_lookup(t->t2, fp);
}


void active_filter(struct segment *s) {
	TIMER_DECLARE(1);
	TIMER_BEGIN(1);
	GSequenceIter *iter = g_sequence_get_begin_iter(s->chunks);
	GSequenceIter *end = g_sequence_get_end_iter(s->chunks);
	for (; iter!=end; iter=g_sequence_iter_next(iter)) {
		struct chunk* c = g_sequence_get(iter);
		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END))
			continue;

		ac_record.data_size += c->size;

		struct cache_entry* ce = double_filter_lookup(ac_table, &c->fp);
		if(ce) {
			SET_CHUNK(c, CHUNK_DUPLICATE);
			ac_record.dedup_size += c->size;
		} else {
			SET_CHUNK(c, CHUNK_UNIQUE);
			ac_record.stored_size += c->size;
			
			struct cache_entry *newce = (struct cache_entry *)malloc(sizeof(struct cache_entry));
			memcpy(&newce->fp, &c->fp, sizeof(fingerprint));
			newce->flag = jcr.id;
			newce->size = c->size;
			// the thread is blocking if we wairt for the data writing
			TIMER_END(1, ac_record.dedup_time);
			newce->id = active_insert(c);
			//newce->id = TEMPORARY_ID;
			TIMER_DECLARE(1);
			TIMER_BEGIN(1);

			g_hash_table_insert(ac_table->t2, &newce->fp, newce);
			ac_table->t2size++;

			//active_filter_insert(ac_table, c);
		}	
	}
	TIMER_END(1, ac_record.dedup_time);
}

// =========================== active_insert thread ========================
containerid active_insert(struct chunk *c){
	if (active_container == NULL) {
		active_container = create_active_container();
	}
	if(container_overflow(active_container, c->size)) {
		write_active_container_async(active_container);
		active_container = create_active_container();
	}
	if(add_chunk_to_container(active_container, c)) {
        return c->id;
	}

	WARNING("Fail to insert the chunk into active container!");
	exit(1);
}


// ==================================== active containers ============================

void active_remove(struct doubleBuffer *t){
	if(ac_cache) g_hash_table_destroy(ac_cache);
	ac_cache = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal, NULL, free_chunk);
	
	struct lruCache *cache;
	if (destor.simulation_level >= SIMULATION_RESTORE)
		cache = new_lru_cache(destor.restore_cache[1], free_container_meta,
				lookup_fingerprint_in_container_meta);
	else
		cache = new_lru_cache(destor.restore_cache[1], free_container,
				lookup_fingerprint_in_container);

	// the chunks in T1 are cold chunks
	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, t->t1);
	while(g_hash_table_iter_next(&iter, &key, &value)){
		// read data from active container and write the data in archive container
		struct cache_entry *victim = (struct cache_entry *)value;
		
		if (destor.simulation_level >= SIMULATION_RESTORE) {
			struct containerMeta *cm = lru_cache_lookup(cache, &victim->fp);
			if (!cm) {
				VERBOSE("Remove cold chunks cache: container %lld is missed", victim->id);
				cm = retrieve_active_container_meta_by_id(victim->id);
				assert(lookup_fingerprint_in_container_meta(cm, &victim->fp));
				lru_cache_insert(cache, cm, NULL, NULL);
			}
		} else {
			struct container *con = lru_cache_lookup(cache, &victim->fp);
			if (!con) {
				VERBOSE("Remove cold chunks cache: container %lld is missed", victim->id);
				con = retrieve_active_container_by_id(victim->id);
				//con = retrieve_active_pool_by_id(victim->id);
				lru_cache_insert(cache, con, NULL, NULL);
			}
			struct chunk *ck = get_chunk_in_container(con, &victim->fp);
			assert(ck);
			// check the correctness
			SHA_CTX ctx;
			SHA_Init(&ctx);
			SHA_Update(&ctx, ck->data, ck->size);
			SHA_Final(ck->fp, &ctx);
			assert(memcmp(&ck->fp, &victim->fp, sizeof(fingerprint))==0);

			// update the metadata of active container
			update_cons(con, ck);

			//write to archive_container 
			if(archive_container == NULL) {
				archive_container = create_archive_container();
			}
			if(container_overflow(archive_container, ck->size)){
				printf("%ldth archive_countainer is full !\n", archive_container->meta.id);
				write_archive_container_async(archive_container);
				archive_container = create_archive_container();
			}
			if(add_chunk_to_container(archive_container, ck)) {
				struct chunk* wc = new_chunk(0);
	            memcpy(&wc->fp, &ck->fp, sizeof(fingerprint));
	            wc->id = ck->id;
	            assert(wc->id > 0);
	            g_hash_table_insert(ac_cache, &wc->fp, wc);
			}
		}
		/*if(memcmp(&ck->fp, &victim->fp, sizeof(fingerprint))!=0) {
			char code[41];
			hash2code(ck->fp, code);
			code[40] = 0;
			printf("chunk : %s, %d\n", code, ck->size);
			
			hash2code(victim->fp, code);
			code[40] = 0;
			printf("victim: %s, %d\n", code, victim->size);

		}*/

	}
	if (archive_container && !container_empty(archive_container)) {

		write_archive_container_async(archive_container);
	}
	free_lru_cache(cache);

	merge_cons(ac_table->t2);
	update_recipe(ac_cache);
}






/* 
 *================== count the chunk number ===========================
 */

void init_bitmap(){
	assert(ACTIVE_SIZE%(PAGE_SIZE*32) == 0);

	sds bitmapfile = sdsdup(destor.working_directory);
	bitmapfile = sdscat(bitmapfile, "active/bitmap");

	b = (struct bitmap *)malloc(sizeof(struct bitmap));
	FILE *fp;
	if((fp=fopen(bitmapfile, "r"))) {
		fread(&(b->max_size), sizeof(int), 1, fp);
		fread(&(b->ideal_size), sizeof(int), 1, fp);
		fread(&(b->int_number), sizeof(int), 1, fp);

		int n=b->int_number, i=0;
		b->bitmap = (int *)malloc(n*sizeof(int));

		for(i=0; i<n; i++){
			fread(&(b->bitmap[i]), sizeof(int), 1, fp);
		}
		fclose(fp);
	} else{
		int total_page = ACTIVE_SIZE/PAGE_SIZE;
		b->max_size = total_page;
		b->ideal_size = total_page;

		int n = total_page/32, i;
		b->int_number = n;
		b->bitmap = (int *)malloc(n*sizeof(int));
		for(i=0; i<n; i++)
			b->bitmap[i]=0;
	}

	sdsfree(bitmapfile);

	NOTICE("Init active_bitmap total_size: %d, ideal_size=%d, int_number=%d", 
		b->max_size, b->ideal_size, b->int_number);

}


void init_active_cache() {
	sds activepath = sdsdup(destor.working_directory);
	activepath =  sdscat(activepath, "active/cache");

	//ac_cache = new_lru_cache(-1, free_entry, cache_entry_equal);
	ac_cache = g_hash_table_new_full(g_int_hash, g_fingerprint_equal, NULL, free);
	FILE *fp;
	if ((fp=fopen(activepath, "r"))) {
		
		int key_num;
		fread(&key_num, sizeof(int), 1, fp);
		for(; key_num>0; key_num--){
			struct cache_entry *ce = (struct cache_entry *)malloc(sizeof(struct cache_entry));
			fread(&ce->fp, destor.index_key_size, 1, fp);
			fread(&ce->flag, sizeof(int32_t), 1, fp);
			fread(&ce->size, sizeof(int32_t), 1, fp);
			ce->id = 0;

			g_hash_table_insert(ac_cache, &ce->fp, ce);

		}
		fclose(fp);
	}
	sdsfree(activepath);

	NOTICE("Init active cache size: %d", g_hash_table_size(ac_cache));

}

void close_active_cache() {
	sds activepath = sdsdup(destor.working_directory);
	activepath =  sdscat(activepath, "active/cache");

	FILE *fp;
	if ((fp=fopen(activepath, "w")) == NULL) {
		perror("Can not open active/cache for write because:");
		exit(1);
	}

	NOTICE("flushing active cache!");
	int key_num = g_hash_table_size(ac_cache);
	if(fwrite(&key_num, sizeof(int), 1, fp) != 1){
		perror("Fail to write a key_num!");
		exit(1);
	}

	
	int ind[jcr.id+1], i=0;
	for(i=0; i<=jcr.id; i++) ind[i] = 0;

	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, ac_cache);
	while(g_hash_table_iter_next(&iter, &key, &value)) {
		struct cache_entry *ce = (struct cache_entry *)value;
		if(fwrite(&ce->fp, destor.index_key_size, 1, fp) != 1){
			perror("Fail to write a key!");
			exit(1);
		}

		if(fwrite(&ce->flag, sizeof(int32_t), 1, fp) != 1) {
			perror("Fail to write a flag!");
			exit(1);
		}

		if(fwrite(&ce->size, sizeof(int32_t), 1, fp) != 1) {
			perror("Fail to write a size!");
			exit(1);
		}

		ind[ce->flag]++;
	}

		
	fclose(fp);

	sdsfree(activepath);

	for(i=0; i<=jcr.id; i++) {
		printf("%d ", ind[i]);
		if((i+1)%50==0) printf("\n");
	}
	printf("\n");
}


void close_bitmap() {
	sds bitmapfile = sdsdup(destor.working_directory);
	bitmapfile = sdscat(bitmapfile, "active/bitmap");

	FILE *fp;
	if ((fp=fopen(bitmapfile, "w")) == NULL) {
		perror("Can not open active/cache for write because:");
		exit(1);
	}

	NOTICE("flushing active bitmap, max_size: %d, ideal_size: %d", b->max_size, b->ideal_size);
	if(fwrite(&(b->max_size), sizeof(int), 1, fp) != 1){
		perror("Fail to write b->max_size!");
		exit(1);
	}
	if(fwrite(&(b->ideal_size), sizeof(int), 1, fp) != 1){
		perror("Fail to write b->ideal_size!");
		exit(1);
	}
	if(fwrite(&(b->int_number), sizeof(int), 1, fp) != 1){
		perror("Fail to write b->int_number!");
		exit(1);
	}
	int n = b->int_number, i;
	for(i=0; i<n; i++) {
		if(fwrite(&(b->bitmap[i]), sizeof(int), 1, fp) != 1){
			perror("Fail to write b->bitmap!");
			exit(1);
		}
	}

	fclose(fp);

	sdsfree(bitmapfile);
}

void statistic_index(struct segment* s) {
	TIMER_DECLARE(1);
	TIMER_BEGIN(1);
	GSequenceIter *iter = g_sequence_get_begin_iter(s->chunks);
	GSequenceIter *end = g_sequence_get_end_iter(s->chunks);
	for (; iter!=end; iter=g_sequence_iter_next(iter)) {
		struct chunk* c = g_sequence_get(iter);
		if (CHECK_CHUNK(c, CHUNK_FILE_START) || CHECK_CHUNK(c, CHUNK_FILE_END))
			continue;

		ac_record.data_size += c->size;

		struct cache_entry* ce = g_hash_table_lookup(ac_cache, &c->fp);
		if(ce) {
			jcr.unique_data_size += c->size;
			ac_record.dedup_size += c->size;
			ce->flag = jcr.id;

		} else {
			ac_record.stored_size += c->size;
			
			struct cache_entry *newce = (struct cache_entry *)malloc(sizeof(struct cache_entry));
			memcpy(&newce->fp, &c->fp, sizeof(fingerprint));
			newce->flag = jcr.id;
			newce->size = 0;
			newce->id = 0;
			
			g_hash_table_insert(ac_cache, &newce->fp, newce);
		}	
	}
	TIMER_END(1, ac_record.dedup_time);
}


/*
 * ======================== use the fixed pages ===============================
 */
void active_filter_insert(struct doubleBuffer *t, struct chunk *ck) {
	/*int required_pages = ceil(ck->size/(float)PAGE_SIZE);
	assert(b->ideal_size >= required_pages);

	struct cache_entry *newce = (struct cache_entry *)malloc(sizeof(struct cache_entry));
	memcpy(&newce->fp, &ck->fp, sizeof(fingerprint));
	newce->flag = jcr.id;
	newce->size = ck->size;
	if(required_pages == 0) {
		newce->pages = NULL;
	} else {
		int i,j,k,n=0;
		int off = ck->size%PAGE_SIZE;
		unsigned int* pages = (unsigned int*)malloc(required_pages*sizeof(unsigned int));
		for(i=0; i<b->int_number; i++) {
			// find ideal locations and write the data to active
			if(b->bitmap[i]==0xffffffff) continue;
			for (j=31; j>=0; j--){
				if(!((b->bitmap[i]>>j)&1)) {
					pages[n] = i<<16 | j;
					b->bitmap[i] |= 1<<j;
					b->ideal_size--;
				
					// 写数据
					k = i*32 + 31-j;
					if(n+1>=required_pages && off > 0) {
						fseek(ac_fp, k*PAGE_SIZE, SEEK_SET);
						fwrite(ck->data + n*PAGE_SIZE, off, 1, ac_fp);
					}
					else{
						fseek(ac_fp, k*PAGE_SIZE, SEEK_SET);
						fwrite(ck->data + n*PAGE_SIZE, PAGE_SIZE, 1, ac_fp);
					}
					n++;
				}
				if(n>=required_pages) break;
			}
			if(n>=required_pages) break;
		}
		newce->pages = &pages[0];
	}
			
	g_hash_table_insert(t->t2, &newce->fp, newce);
	t->t2size++;*/
}


struct chunk* read_active_chunk(struct cache_entry *victim) {
	struct chunk* ck = new_chunk(victim->size);
		
	/*int off = victim->size % PAGE_SIZE;
	int required_pages = ceil(victim->size/(float)PAGE_SIZE), i, j, k, n;
	for (n=0; n<required_pages; n++) {
		// read the pages, set bit=0 
		i = victim->pages[n] >> 16;
		j = victim->pages[n] & 0x0000ffff;
		//b->bitmap[i] &= ~(1<<j);
		//b->ideal_size++;

		//construct the data
		k = i*32 + 31-j;
		char tempbuffer[PAGE_SIZE];
		if(n+1>=required_pages && off > 0) {
			fseek(ac_fp, k*PAGE_SIZE, SEEK_SET);
			fread(tempbuffer, off, 1, ac_fp);
			memcpy(ck->data + n*PAGE_SIZE, tempbuffer, off);
		} 
		else{
			fseek(ac_fp, k*PAGE_SIZE, SEEK_SET);
			fread(tempbuffer, PAGE_SIZE, 1, ac_fp);
			memcpy(ck->data + n*PAGE_SIZE, tempbuffer, PAGE_SIZE);
		}
	}
	// check the correctness
	SHA_CTX ctx;
	SHA_Init(&ctx);
	SHA_Update(&ctx, ck->data, ck->size);
	SHA_Final(ck->fp, &ctx);

	if(memcmp(&ck->fp, &victim->fp, sizeof(fingerprint))!=0) {
		char code[41];
		hash2code(ck->fp, code);
		code[40] = 0;
		printf("chunk : %s, %d\n", code, ck->size);
			
		hash2code(victim->fp, code);
		code[40] = 0;
		printf("victim: %s, %d\n", code, victim->size);

	}

	assert(memcmp(&ck->fp, &victim->fp, sizeof(fingerprint))==0);*/

	return ck;
}


void active_filter_kicks(struct doubleBuffer *t) {
	/*g_hash_table_destroy(ac_cache);
	ac_cache = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal, NULL, free_chunk);

	GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, t->t1);
	while(g_hash_table_iter_next(&iter, &key, &value)){
		// kicks the bitmap and write the data
		struct cache_entry *victim = (struct cache_entry *)value;
		struct chunk* ck = new_chunk(victim->size);
		
		int off = victim->size % PAGE_SIZE;
		int required_pages = ceil(victim->size/(float)PAGE_SIZE), i, j, k, n;
		for (n=0; n<required_pages; n++) {
			// read the pages, set bit=0 
			i = victim->pages[n] >> 16;
			j = victim->pages[n] & 0x0000ffff;
			b->bitmap[i] &= ~(1<<j);
			b->ideal_size++;

			//construct the data
			k = i*32 + 31-j;
			char tempbuffer[PAGE_SIZE];
			if(n+1>=required_pages && off > 0) {
				fseek(ac_fp, k*PAGE_SIZE, SEEK_SET);
				fread(tempbuffer, off, 1, ac_fp);
				memcpy(ck->data + n*PAGE_SIZE, tempbuffer, off);
			} 
			else{
				fseek(ac_fp, k*PAGE_SIZE, SEEK_SET);
				fread(tempbuffer, PAGE_SIZE, 1, ac_fp);
				memcpy(ck->data + n*PAGE_SIZE, tempbuffer, PAGE_SIZE);
			}
		}
		// check the correctness
		SHA_CTX ctx;
		SHA_Init(&ctx);
		SHA_Update(&ctx, ck->data, ck->size);
		SHA_Final(ck->fp, &ctx);

		/*if(memcmp(&ck->fp, &victim->fp, sizeof(fingerprint))!=0) {
			char code[41];
			hash2code(ck->fp, code);
			code[40] = 0;
			printf("chunk : %s, %d\n", code, ck->size);
			
			hash2code(victim->fp, code);
			code[40] = 0;
			printf("victim: %s, %d\n", code, victim->size);

		}*/

		/*assert(memcmp(&ck->fp, &victim->fp, sizeof(fingerprint))==0);
		//write to archive_container 
		if(archive_container == NULL) {
			archive_container = create_archive_container();
			//container_chunks = g_sequence_new(free_chunk);
		}
		if(container_overflow(archive_container, ck->size)){
			printf("%ldth archive_countainer is full !\n", archive_container->meta.id);
			/*GHashTable *features = sampling(container_chunks,
                        		g_sequence_get_length(container_chunks));
            index_update(features, get_container_id(archive_container));
            g_hash_table_destroy(features);
            g_sequence_free(container_chunks);
            container_chunks = g_sequence_new(free_chunk);*/

			/*write_archive_container_async(archive_container);
			archive_container = create_archive_container();
		}
		if(add_chunk_to_container(archive_container, ck)) {
			struct chunk* wc = new_chunk(0);
            memcpy(&wc->fp, &ck->fp, sizeof(fingerprint));
            wc->id = ck->id;
            assert(wc->id > 0);
            g_hash_table_insert(ac_cache, &wc->fp, wc);
            //g_sequence_append(container_chunks, ck);
		}
	}
	if (archive_container && !container_empty(archive_container)) {
		/*GHashTable *features = sampling(container_chunks,
                        		g_sequence_get_length(container_chunks));
        index_update(features, get_container_id(archive_container));
        g_hash_table_destroy(features);
        g_sequence_free(container_chunks);
        container_chunks = g_sequence_new(free_chunk);*/

		/*write_archive_container_async(archive_container);
	}

	update_recipe(ac_cache);*/
}


