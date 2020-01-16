/*
 * create on: Oct 31, 2019
 *	  Auther: Pengfei Li
 */

#include "active.h"
#include "../storage/containerstore.h"
#include "../jcr.h"
#include "../backup.h"
#include "../index/index.h"
#include "../recipe/recipestore.h"


static pthread_t active_recipe_t;

/* the write buffer of recipe meta */
static int metabufsize = 64*1024;

/* the write buffer of records */
static int recordbufsize = 64*1024;




int active_backup_version_exists(int number) {
	sds recipepath = sdsdup(destor.working_directory);
	recipepath = sdscat(recipepath, "/active/recipes/");

	sds fname = sdsdup(recipepath);
	fname = sdscat(fname, "bv");
	char s[20];
	sprintf(s, "%d", number);
	fname = sdscat(fname, s);
	fname = sdscat(fname, ".meta");

	if (access(fname, 0) == 0) {
		sdsfree(fname);
		return 1;
	}
	sdsfree(fname);
	return 0;
}

struct backupVersion* open_active_backup_version(int number) {

	if (!active_backup_version_exists(number)) {
		fprintf(stderr, "Backup version %d doesn't exist", number);
		exit(1);
	}

	struct backupVersion *b = (struct backupVersion *) malloc(
			sizeof(struct backupVersion));

	sds recipepath = sdsdup(destor.working_directory);
	recipepath = sdscat(recipepath, "/active/recipes/");

	b->fname_prefix = sdsdup(recipepath);
	b->fname_prefix = sdscat(b->fname_prefix, "bv");
	char s[20];
	sprintf(s, "%d", number);
	b->fname_prefix = sdscat(b->fname_prefix, s);

	sds fname = sdsdup(b->fname_prefix);
	fname = sdscat(fname, ".meta");
	if ((b->metadata_fp = fopen(fname, "r")) == 0) {
		fprintf(stderr, "Can not open bv%d.meta!\n", b->bv_num);
		exit(1);
	}

	fseek(b->metadata_fp, 0, SEEK_SET);
	fread(&b->bv_num, sizeof(b->bv_num), 1, b->metadata_fp);
	assert(b->bv_num == number);
	fread(&b->deleted, sizeof(b->deleted), 1, b->metadata_fp);

	if (b->deleted) {
		/*fprintf(stderr, "Backup version %d has been deleted!\n", number);*/
        NOTICE("This version has been deleted!\n");
		/*exit(1);*/
	}

	fread(&b->number_of_files, sizeof(b->number_of_files), 1, b->metadata_fp);
	fread(&b->number_of_chunks, sizeof(b->number_of_chunks), 1, b->metadata_fp);

	int pathlen;
	fread(&pathlen, sizeof(int), 1, b->metadata_fp);
	char path[pathlen + 1];
	fread(path, pathlen, 1, b->metadata_fp);
	path[pathlen] = 0;
	b->path = sdsnew(path);

	fname = sdscpy(fname, b->fname_prefix);
	fname = sdscat(fname, ".recipe");
	if ((b->recipe_fp = fopen(fname, "r")) <= 0) {
		fprintf(stderr, "Can not open bv%d.recipe!\n", b->bv_num);
		exit(1);
	}

	fname = sdscpy(fname, b->fname_prefix);
	fname = sdscat(fname, ".records");
	if ((b->record_fp = fopen(fname, "r")) <= 0) {
		fprintf(stderr, "Can not open bv%d.records!\n", b->bv_num);
		exit(1);
	}

	b->metabuf = 0;
	b->metabufoff = 0;

	b->recordbuf = 0;
	b->recordbufoff = 0;

	sdsfree(fname);

	return b;
}




struct backupVersion* create_active_backup_version(const char *path) {
	struct backupVersion *b = (struct backupVersion *) malloc(
			sizeof(struct backupVersion));

	b->bv_num = jcr.id;
	b->path = sdsnew(path);

	/*
	 * If the path points to a file,
	 */
	int cur = sdslen(b->path) - 1;
	while (b->path[cur] != '/') {
		b->path[cur] = 0;
		cur--;
	}
	sdsupdatelen(b->path);

	b->deleted = 0;
	b->number_of_chunks = 0;
	b->number_of_files = 0;

	sds recipepath = sdsdup(destor.working_directory);
	recipepath = sdscat(recipepath, "/active/recipes/");

	b->fname_prefix = sdsdup(recipepath);
	b->fname_prefix = sdscat(b->fname_prefix, "bv");
	char s[20];
	sprintf(s, "%d", b->bv_num);
	b->fname_prefix = sdscat(b->fname_prefix, s);

	sds fname = sdsdup(b->fname_prefix);
	fname = sdscat(fname, ".meta");
	if ((b->metadata_fp = fopen(fname, "w")) == 0) {
		fprintf(stderr, "Can not create bv%d.meta!\n", b->bv_num);
		exit(1);
	}

	fseek(b->metadata_fp, 0, SEEK_SET);
	fwrite(&b->bv_num, sizeof(b->bv_num), 1, b->metadata_fp);
	fwrite(&b->deleted, sizeof(b->deleted), 1, b->metadata_fp);

	fwrite(&b->number_of_files, sizeof(b->number_of_files), 1, b->metadata_fp);
	fwrite(&b->number_of_chunks, sizeof(b->number_of_chunks), 1,
			b->metadata_fp);

	int pathlen = sdslen(b->path);
	fwrite(&pathlen, sizeof(pathlen), 1, b->metadata_fp);
	fwrite(b->path, sdslen(b->path), 1, b->metadata_fp);

	b->metabuf = malloc(metabufsize);
	b->metabufoff = 0;

	fname = sdscpy(fname, b->fname_prefix);
	fname = sdscat(fname, ".recipe");
	if ((b->recipe_fp = fopen(fname, "w+")) <= 0) {
		fprintf(stderr, "Can not create bv%d.recipe!\n", b->bv_num);
		exit(1);
	}

	b->recordbuf = malloc(recordbufsize);
	b->recordbufoff = 0;

	fname = sdscpy(fname, b->fname_prefix);
	fname = sdscat(fname, ".records");
	if ((b->record_fp = fopen(fname, "w")) <= 0) {
		fprintf(stderr, "Can not create bv%d.records!\n", b->bv_num);
		exit(1);
	}

	sdsfree(fname);
	sdsfree(recipepath);

	return b;
}

void init_active_backupVersion(char* path) {
	active_bv = create_active_backup_version(path);
}

void append_segment_recipe(struct backupVersion* b, int flag, int segment_size){
	assert(flag == CHUNK_SEGMENT_START || flag == CHUNK_SEGMENT_END);

	fseek(b->recipe_fp, 0, SEEK_END);
	int64_t off = ftell(b->recipe_fp);

	if(flag == CHUNK_SEGMENT_START){
		/* Two flags and many chunk pointers */
		b->segmentlen = segment_size * (sizeof(fingerprint) + sizeof(containerid) + sizeof(int32_t));
		b->segmentbuf = malloc(b->segmentlen);
		b->segmentbufoff = 0;
	}

	if(flag == CHUNK_SEGMENT_END){
		VERBOSE("Filter phase: write a segment start at offset %lld!", off);
		fwrite(b->segmentbuf, b->segmentlen, 1, b->recipe_fp);
		free(b->segmentbuf);
		b->segmentbuf = NULL;
		b->segmentlen = 0;
	}

}


//========================= insert into active conatiner and record recipe================
void *active_recipe_thread(void *arg) {
	struct fileRecipeMeta* r = NULL;

	while (1) {
		struct chunk *c = sync_queue_pop(active_dedup_queue);
		
		if (c == NULL)
			break;

		 /* reconstruct a segment */
        struct segment* s = new_segment();

		/* segment head */
        assert(CHECK_CHUNK(c, CHUNK_SEGMENT_START));
        free_chunk(c);

        c = sync_queue_pop(active_dedup_queue);
        while (!(CHECK_CHUNK(c, CHUNK_SEGMENT_END))) {
            g_sequence_append(s->chunks, c);
            if (!CHECK_CHUNK(c, CHUNK_FILE_START)
                    && !CHECK_CHUNK(c, CHUNK_FILE_END))
                s->chunk_num++;

            c = sync_queue_pop(active_dedup_queue);
        }
        free_chunk(c);


        /* Write a SEGMENT_BEGIN */
        append_segment_recipe(active_bv, CHUNK_SEGMENT_START, s->chunk_num);

        /* Write recipe */
    	GSequenceIter *iter = g_sequence_get_begin_iter(s->chunks);
    	GSequenceIter *end = g_sequence_get_end_iter(s->chunks);
        for (; iter != end; iter = g_sequence_iter_next(iter)) {
            c = g_sequence_get(iter);
            // write into active container and update ac_table 
            /*if(CHECK_CHUNK(c, CHUNK_UNIQUE)){
            	int id = active_insert(c);
				struct cache_entry *newce = (struct cache_entry *)malloc(sizeof(struct cache_entry));
				memcpy(&newce->fp, &c->fp, sizeof(fingerprint));
				newce->flag = jcr.id;
				newce->size = c->size;
				newce->id = id;
				g_hash_table_insert(update_t2, &newce->fp, newce);
            }*/

            // record the recipe
        	if(r == NULL){
        		assert(CHECK_CHUNK(c,CHUNK_FILE_START));
        		r = new_file_recipe_meta(c->data);
        	}else if(!CHECK_CHUNK(c,CHUNK_FILE_END)){
        		struct chunkPointer cp;
        		cp.id = 0;							 // use 0 to represent the active area
        		memcpy(&cp.fp, &c->fp, sizeof(fingerprint));
        		cp.size = c->size;
        		assert(g_hash_table_contains(ac_table->t2, &(cp.fp)));
        		append_n_chunk_pointers(active_bv, &cp ,1);
        		r->chunknum++;
        		r->filesize += c->size;

    	    	jcr.chunk_num++;
	    	    jcr.data_size += c->size;

        	}else{
        		assert(CHECK_CHUNK(c,CHUNK_FILE_END));
        		append_file_recipe_meta(active_bv, r);
        		free_file_recipe_meta(r);
        		r = NULL;

	            jcr.file_num++;
        	}
        }
        /* Write a SEGMENT_END */
       	append_segment_recipe(active_bv, CHUNK_SEGMENT_END, 0);
	}

	jcr.status = JCR_STATUS_DONE;

	return NULL;
}



void start_active_recipe_phase() {
	active_recipe_queue = sync_queue_new(1000);

	pthread_create(&active_recipe_t, NULL, active_recipe_thread, NULL);
}

void stop_active_recipe_phase() {
	pthread_join(active_recipe_t, NULL);
	/*NOTICE("active_dedup phase stops successfully: %d segments of %d chunks on average",
			segment_num, segment_num ? chunk_num / segment_num : 0);*/

	/*GHashTableIter iter;
	gpointer key, value;
	g_hash_table_iter_init(&iter, update_t2);
	while(g_hash_table_iter_next(&iter, &key, &value)){
		struct cache_entry *ce = (struct cache_entry *)value;
		struct cache_entry *oldce = g_hash_table_lookup(ac_table->t2, &ce->fp);
		assert(oldce);
		oldce->id = ce->id;
	}
	g_hash_table_destroy(update_t2);*/
}


// ======================== update previous recipe ========================
struct recipeinfo *create_recipeinfo(int bv_num, sds filename) {
	struct recipeinfo *ri = (struct recipeinfo *)malloc(sizeof(struct recipeinfo));
	sds recipepath = sdsdup(destor.working_directory);
	recipepath = sdscat(recipepath, filename);
	char s[20];
	sprintf(s, "%d", bv_num);
	recipepath = sdscat(recipepath, s);
	recipepath = sdscat(recipepath, ".recipe");
	
	if ((ri->recipe_fp = fopen(recipepath, "w")) == 0) {
		fprintf(stderr, "Can not open %s!\n", recipepath);
		exit(1);
	}
	ri->segmentbuf = NULL;
	ri->segmentlen = 1024 * (sizeof(fingerprint) + sizeof(containerid) + sizeof(int32_t));
	ri->segmentbufoff = 0;

	ri->number_of_chunks = 0;

	printf("Create recipe: %s\n", recipepath);
	sdsfree(recipepath);
	return ri;
}

void free_recipeinfo(struct recipeinfo *ri) {
	if(ri->segmentbuf){
		free(ri->segmentbuf);
		ri->segmentbuf=0;
	}
	if(ri->recipe_fp)
		fclose(ri->recipe_fp);
	
	free(ri);
}

void close_recipeinfo(struct recipeinfo *ri, struct backupVersion* bv) {
	if(ri->segmentbuf){
		fseek(ri->recipe_fp, 0, SEEK_END);
		fwrite(ri->segmentbuf, ri->segmentbufoff, 1, ri->recipe_fp);
		free(ri->segmentbuf);
		ri->segmentbuf = NULL;
		ri->segmentbufoff = 0;
	}

	fseek(ri->recipe_fp, 0, SEEK_END);
	int off1 = ftell(ri->recipe_fp);
	fseek(bv->recipe_fp, 0, SEEK_END);
	int off2 = ftell(bv->recipe_fp);
	//printf("%d == %d\n", off1, off2);
	assert(off1 == off2);
	assert(ri->number_of_chunks == bv->number_of_chunks);

	free_recipeinfo(ri);
}

void write_recipe(struct recipeinfo *ri, struct chunkPointer* cp) {

	if(!ri->segmentbuf){
		ri->segmentbuf = malloc(ri->segmentlen);
		ri->segmentbufoff = 0;
	}

	memcpy(ri->segmentbuf + ri->segmentbufoff, &cp->fp, sizeof(fingerprint));
	ri->segmentbufoff += sizeof(fingerprint);
	memcpy(ri->segmentbuf + ri->segmentbufoff, &cp->id, sizeof(containerid));
	ri->segmentbufoff += sizeof(containerid);
	memcpy(ri->segmentbuf + ri->segmentbufoff, &cp->size, sizeof(int32_t));
	ri->segmentbufoff += sizeof(int32_t);

	ri->number_of_chunks++;

	if(ri->segmentbufoff >= ri->segmentlen){
		fseek(ri->recipe_fp, 0, SEEK_END);

		fwrite(ri->segmentbuf, ri->segmentlen, 1, ri->recipe_fp);
		free(ri->segmentbuf);
		ri->segmentbuf = NULL;
		ri->segmentbufoff = 0;
	}

}


void update_recipe(GHashTable *t) {
	if (!active_backup_version_exists(jcr.id - 1)) {
		fprintf(stderr, "Previous backup version %d doesn't exist\n", jcr.id - 1);
		return ;
	}
	struct backupVersion* tmpbv = open_active_backup_version(jcr.id - 1);
	sds filename = "/active/recipes/nbv";
	struct recipeinfo* ri = create_recipeinfo(jcr.id-1, filename);

	int i, j, k;
	for (i = 0; i < tmpbv->number_of_files; i++) {
		struct fileRecipeMeta *r = read_next_file_recipe_meta(tmpbv);
		for (j = 0; j < r->chunknum; j++) {
			struct chunkPointer* cp = read_next_n_chunk_pointers(tmpbv, 1, &k);
			struct chunk* ck = g_hash_table_lookup(t, &cp->fp);
			if(ck) {
				// update the id
				cp->id = ck->id;
				assert(cp->id > 0);
			} else {
				cp->id = -jcr.id;
			}
			write_recipe(ri, cp);
			free(cp);
		}
		
		free_file_recipe_meta(r);
	}

	close_recipeinfo(ri, tmpbv);
	free_backup_version(tmpbv);

}


struct recipeinfo *open_recipeinfo(struct backupVersion* b, sds filename) {
	struct recipeinfo *ri = (struct recipeinfo *)malloc(sizeof(struct recipeinfo));
	sds recipepath = sdsdup(destor.working_directory);
	recipepath = sdscat(recipepath, filename);
	char s[20];
	sprintf(s, "%d", b->bv_num);
	recipepath = sdscat(recipepath, s);
	recipepath = sdscat(recipepath, ".recipe");
	
	if ((ri->recipe_fp = fopen(recipepath, "r")) == 0) {
		fprintf(stderr, "Can not open %s!\n", recipepath);
		exit(1);
	}
	ri->segmentbuf = NULL;
	ri->segmentlen = 0;
	ri->segmentbufoff = 0;

	//int64_t off = ftell(ri->recipe_fp);

	ri->number_of_chunks = b->number_of_chunks;
	printf("Open recipepath: %s\n", recipepath);
	sdsfree(recipepath);

	return ri;
}



//================================ read the updated chunk pointer =====================
struct chunkPointer* active_read_next_chunk_pointers(struct recipeinfo *ri) {

	/* Total number of read chunks. */
	static int read_chunk_num;

	if (read_chunk_num == ri->number_of_chunks) {
		/* It's the stream end. */
		return NULL;
	}

	struct chunkPointer *cp = (struct chunkPointer *) malloc(
			sizeof(struct chunkPointer));

	fread(&(cp->fp), sizeof(fingerprint), 1, ri->recipe_fp);
	fread(&(cp->id), sizeof(containerid), 1, ri->recipe_fp);
	fread(&(cp->size), sizeof(int32_t), 1, ri->recipe_fp);

	read_chunk_num++;
	assert(read_chunk_num <= ri->number_of_chunks);

	return cp;
}


struct chunkPointer* active_read_next_chunk_pointers_all(struct recipeinfo *ri, int *read_chunk_num) {

	struct chunkPointer *cp = (struct chunkPointer *) malloc(
			sizeof(struct chunkPointer));

	fread(&(cp->fp), sizeof(fingerprint), 1, ri->recipe_fp);
	fread(&(cp->id), sizeof(containerid), 1, ri->recipe_fp);
	fread(&(cp->size), sizeof(int32_t), 1, ri->recipe_fp);

	assert(*read_chunk_num <= ri->number_of_chunks);
	(*read_chunk_num)++;

	return cp;
}




// ============= update all recipes =========================
struct fileRecipeMeta* active_read_next_file_recipe_meta(struct backupVersion* b, int read_file_num) {

	assert(read_file_num <= b->number_of_files);

	int len;
	fread(&len, sizeof(len), 1, b->metadata_fp);
	char filename[len + 1];

	fread(filename, len, 1, b->metadata_fp);
	filename[len] = 0;

	struct fileRecipeMeta* r = new_file_recipe_meta(filename);

	fread(&r->chunknum, sizeof(r->chunknum), 1, b->metadata_fp);
	fread(&r->filesize, sizeof(r->filesize), 1, b->metadata_fp);

	return r;
}

extern struct container* retrieve_archive_container_by_id(containerid id);

GHashTable* update_next_recipe(GHashTable *t, int id) {
	printf("hash table size: %d\n", g_hash_table_size(t));
	if (!active_backup_version_exists(id)) {
		fprintf(stderr, "Previous active backup version %d doesn't exist", id);
		return t;
	}
	struct backupVersion* tmpbv = open_active_backup_version(id);
	struct recipeinfo* ri = open_recipeinfo(tmpbv, "/active/recipes/nbv");
	struct recipeinfo* newri = create_recipeinfo(id, "/active/recipes/newestbv");

	GHashTable* nt = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal, NULL, free);
	int i, j, k;
	int read_file_num = 0, read_chunk_num = 0;


	// read the file metadata 
	for (i = 0; i < tmpbv->number_of_files; i++) {
		struct fileRecipeMeta *r = active_read_next_file_recipe_meta(tmpbv, read_file_num);
		// read chunksPointers
		for (j = 0; j < r->chunknum; j++) {
			//struct chunkPointer* cp = active_read_next_chunk_pointers(ri);
			struct chunkPointer* cp = active_read_next_chunk_pointers_all(ri, &read_chunk_num);
			if(cp->id <= 0){
				struct chunkPointer* ck = g_hash_table_lookup(t, &cp->fp);
				//assert(ck);
				if(ck)
					cp->id = ck->id;
			}
			// insert into hash_table to update next recipe
			struct chunkPointer* te = g_hash_table_lookup(nt, &cp->fp);
			if(te){
				assert(te->id == cp->id);
			}else{
				g_hash_table_insert(nt, &cp->fp, cp);
			}
			write_recipe(newri, cp);
		}
		
		free_file_recipe_meta(r);
		read_file_num++;
	}
	

	g_hash_table_destroy(t);
	close_recipeinfo(newri, tmpbv);
	free_recipeinfo(ri);
	free_backup_version(tmpbv);
	

	return nt;
}


void update_all_recipe(int newestId) {
	if (!active_backup_version_exists(newestId-1) || !active_backup_version_exists(newestId-2)) {
		fprintf(stderr, "Backup versions %d, %d doesn't exist", newestId-1, newestId-2);
		exit(0);
	}
	double update_time = 0.0;
	TIMER_DECLARE(1);
	TIMER_BEGIN(1);

	GHashTable *t = g_hash_table_new_full(g_int64_hash, g_fingerprint_equal, NULL, free);
	struct backupVersion* tmpbv = open_active_backup_version(newestId-1);
	sds filename = "/active/recipes/nbv";
	struct recipeinfo* ri = open_recipeinfo(tmpbv, filename);

	int i, j, k;
	int read_file_num = 0, read_chunk_num = 0;
	for (i = 0; i < tmpbv->number_of_files; i++) {
		struct fileRecipeMeta *r = active_read_next_file_recipe_meta(tmpbv, read_file_num);
		for (j = 0; j < r->chunknum; j++) {
			//struct chunkPointer* cp = read_next_n_chunk_pointers(tmpbv, 1, &k);
			struct chunkPointer* cp = active_read_next_chunk_pointers_all(ri, &read_chunk_num);
			// insert into hash table to update next recipe
			// It is necessary to judge wheter the hash_table have the cps, or errors
			struct chunkPointer* te = g_hash_table_lookup(t, &cp->fp);
			if(te){
				assert(te->id == cp->id);
			}else{
				g_hash_table_insert(t, &cp->fp, cp);
			}
		}	
		free_file_recipe_meta(r);
		read_file_num++;
	}
	free_recipeinfo(ri);
	free_backup_version(tmpbv);

	int id = newestId-2;
	for (id=newestId-2; id>=0; id--){
		t = update_next_recipe(t, id);
	}
	TIMER_END(1, update_time);
	g_hash_table_destroy(t);

	printf("update time(s): %.3f\n", update_time / 1000000);
}
