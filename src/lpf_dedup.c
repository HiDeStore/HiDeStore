/*
 * create by Pengfei Li
 *		2019.10.10
 */

#include "destor.h"
#include "jcr.h"
#include "utils/sync_queue.h"
#include "index/index.h"
#include "backup.h"
#include "storage/containerstore.h"
#include "lpf/active.h"

/* defined in index.c */
extern struct {
	/* Requests to the key-value store */
	int lookup_requests;
	int update_requests;
	int lookup_requests_for_unique;
	/* Overheads of prefetching module */
	int read_prefetching_units;
}index_overhead;


void lpf_dedup(char *path) {
	printf("==================init recipe, container, index, backup_jcr===========================\n");

	init_recipe_store();
	init_container_store();
	init_index();

	init_backup_jcr(path);

	init_active();
	init_active_backupVersion(path);
	//init_active_backup_jcr(path);
	
	printf("=====================init finish============================\n");

	puts("==== backup begin ====");

	TIMER_DECLARE(1);
	TIMER_BEGIN(1);

    time_t start = time(NULL);
    
	if (destor.simulation_level == SIMULATION_ALL) {
		printf("SIMULATION_LEVEL: all (trace)\n");
		start_read_trace_phase();
	}
	else {
		start_read_phase();
		start_chunk_phase();
		start_hash_phase();
	}
	start_active_dedup_phase();
	start_active_recipe_phase();
	//start_terminate_phase();

	/*do{
        sleep(5);
        fprintf(stderr,"job %" PRId32 ", %" PRId64 " bytes, %" PRId32 " chunks, %d files processed\r", 
                jcr.id, jcr.data_size, jcr.chunk_num, jcr.file_num);
    }while(jcr.status == JCR_STATUS_RUNNING || jcr.status != JCR_STATUS_DONE);
    fprintf(stderr,"job %" PRId32 ", %" PRId64 " bytes, %" PRId32 " chunks, %d files processed\n", 
        jcr.id, jcr.data_size, jcr.chunk_num, jcr.file_num);*/

	if (destor.simulation_level == SIMULATION_ALL) {
		stop_read_trace_phase();
	} else {
		stop_read_phase();
		stop_chunk_phase();
		stop_hash_phase();
	}
	stop_active_dedup_phase();
	stop_active_recipe_phase();
	//stop_terminate_phase();


	TIMER_END(1, jcr.total_time);
	close_index();
	close_container_store();

	close_active();
	update_backup_version(active_bv);
	free_backup_version(active_bv);

	close_recipe_store();

	puts("==== backup end ====");

	printf("number of files: %d\n", jcr.file_num);
	/*printf("number of chunks: %" PRId32 " (%" PRId64 " bytes on average)\n", jcr.chunk_num,
			jcr.data_size / jcr.chunk_num);*/
	

}