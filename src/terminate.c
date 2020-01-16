#include "destor.h"
#include "jcr.h"
#include "backup.h"
#include "restore.h"
#include "lpf/active.h"

static pthread_t terminate_t;

static void* terminate_thread(void *arg) {
	struct chunk* c = NULL;

	while(1) {
		c = sync_queue_pop(hash_queue);
		if (c == NULL)
		{
			break;
		}
		free_chunk(c);
		c=NULL;
	}
	return NULL;
}

void start_terminate_phase() {
	pthread_create(&terminate_t, NULL, terminate_thread, NULL);
}

void stop_terminate_phase() {
	pthread_join(terminate_t, NULL);
	NOTICE("=============Finish the backup phase===================");
}