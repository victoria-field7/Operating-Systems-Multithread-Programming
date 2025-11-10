
#include <dirent.h>
#include <pthread.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>   //allowed library to compress the files
#define BUFFER_SIZE 1048576 // 1MB

#include <zlib.h>
#define MAX_THREADS 19 // We should keep threads under 20 for this project */

/*Group #: 25
Group Members (with NetIDs):
Victoria Field, victoriafield
Armani Romay, armaniromay
Jesse Herrera, herrera42
Jawaad Ramcharitar, jawaadramcharitar (test)
Project Description: We are creating a program that will compress a directory of text files
into a single zipped package with all text files in lexicographical order.
We are going to use multiple threads to compress the files in parallel. The goal for this projectis to
minimize the time it takes to compress the files.
*/

//Used struct for compressed_result_t:
typedef struct {
	unsigned char *data;
	int compressed_size;
	int original_size;
} compressed_result_t;

//Used struct for worker_context_t:
typedef struct {
	char **files;
	int nfiles;
	const char *directory_name;
	int next_index;
	pthread_mutex_t lock;
	compressed_result_t *results;
} worker_context_t;


//Function to check if our file ends with .txt:
static int ends_with_txt(const char *name) {
	size_t len = strlen(name);
	if(len < 4)
		return 0;
	return name[len-4] == '.' && name[len-3] == 't' && name[len-2] == 'x' && name[len-1] == 't';
}

//Helper function to compare two strings:
static int cmp(const void *a, const void *b) {
	return strcmp(*(char * const *) a, *(char * const *) b);
}

//This function builds the full path of our file
static void build_full_path(const char *directory_name, const char *file_name, char *buffer, size_t buffer_size) {
	size_t dir_len = strlen(directory_name);
	int needs_slash = (dir_len > 0 && directory_name[dir_len - 1] == '/') ? 0 : 1;

	if(needs_slash)
		snprintf(buffer, buffer_size, "%s/%s", directory_name, file_name);
	else
		snprintf(buffer, buffer_size, "%s%s", directory_name, file_name);
}


//compress worker here, use mutex locks for critical sections....
static void *worker_thread(void *arg) {
	worker_context_t *ctx = (worker_context_t *) arg;
	for(;;) {
		int my_index;
		// get next index to process
		pthread_mutex_lock(&ctx->lock);
		if(ctx->next_index >= ctx->nfiles) {
			pthread_mutex_unlock(&ctx->lock);
			break;
		}
		my_index = ctx->next_index++;
		pthread_mutex_unlock(&ctx->lock);

		// build full path
		char full_path[4096];
		build_full_path(ctx->directory_name, ctx->files[my_index], full_path, sizeof(full_path));

		// read file
		FILE *f_in = fopen(full_path, "r");
		assert(f_in != NULL);
		unsigned char *buffer_in = (unsigned char *) malloc(BUFFER_SIZE);
		assert(buffer_in != NULL);
		int nbytes = (int) fread(buffer_in, sizeof(unsigned char), BUFFER_SIZE, f_in);
		fclose(f_in);

		// compress
		unsigned char *buffer_out = (unsigned char *) malloc(BUFFER_SIZE);
		assert(buffer_out != NULL);
		z_stream strm;
		int ret = deflateInit(&strm, 9);
		assert(ret == Z_OK);
		strm.avail_in = (uInt) nbytes;
		strm.next_in = buffer_in;
		strm.avail_out = BUFFER_SIZE;
		strm.next_out = buffer_out;

		ret = deflate(&strm, Z_FINISH);
		assert(ret == Z_STREAM_END);
		int nbytes_zipped = (int) (BUFFER_SIZE - strm.avail_out);
		deflateEnd(&strm);

		free(buffer_in);

		// store result
		ctx->results[my_index].data = buffer_out;
		ctx->results[my_index].compressed_size = nbytes_zipped;
		ctx->results[my_index].original_size = nbytes;
	}
	return NULL;
}




//Modidied function compress_directory for multiple threads:
//function starts here:
void compress_directory(char *directory_name) {
	DIR *d;
	struct dirent *dir;
	char **files = NULL;
	int nfiles = 0;
	int capacity = 0;

	d = opendir(directory_name);
	if(d == NULL) {
		printf("An error has occurred\n");
		return;
	}

	/* Build list of .txt files */
	while((dir = readdir(d)) != NULL) {
		if(!ends_with_txt(dir->d_name))
			continue;

		if(nfiles == capacity) {
			int new_capacity = capacity == 0 ? 16 : capacity * 2;
			char **tmp = (char **) realloc(files, (size_t) new_capacity * sizeof(char *));
			assert(tmp != NULL);
			files = tmp;
			capacity = new_capacity;
		}

		files[nfiles] = strdup(dir->d_name);
		assert(files[nfiles] != NULL);
		nfiles++;
	}
	closedir(d);
	qsort(files, nfiles, sizeof(char *), cmp);
	// prepare results and worker context
	compressed_result_t *results = NULL;
	if(nfiles > 0) {
		results = (compressed_result_t *) calloc((size_t) nfiles, sizeof(compressed_result_t));
		assert(results != NULL);
	}

	worker_context_t ctx;
	ctx.files = files;
	ctx.nfiles = nfiles;
	ctx.directory_name = directory_name;
	ctx.next_index = 0;
	ctx.results = results;
	pthread_mutex_init(&ctx.lock, NULL);

	// spawn threads (up to MAX_THREADS). keep total threads <= 20 (main + workers)
	int num_threads = nfiles < MAX_THREADS ? nfiles : MAX_THREADS;
	pthread_t threads[MAX_THREADS];
	for(int t=0; t<num_threads; t++) {
		int rc = pthread_create(&threads[t], NULL, worker_thread, &ctx);
		assert(rc == 0);
	}
	for(int t=0; t<num_threads; t++) {
		pthread_join(threads[t], NULL);
	}
	pthread_mutex_destroy(&ctx.lock);

	// write output in lexicographical order
	int total_in = 0, total_out = 0;
	FILE *f_out = fopen("text.tzip", "w");
	assert(f_out != NULL);
	for(int i=0; i < nfiles; i++) {
		int nbytes_zipped = results[i].compressed_size;
		fwrite(&nbytes_zipped, sizeof(int), 1, f_out);
		fwrite(results[i].data, sizeof(unsigned char), (size_t) nbytes_zipped, f_out);
		total_in += results[i].original_size;
		total_out += nbytes_zipped;
		free(results[i].data);
	}
	fclose(f_out);
	if(nfiles > 0) free(results);

	printf("Compression rate: %.2lf%%\n", 100.0*(total_in-total_out)/(total_in == 0 ? 1 : total_in));

	// release list of files
	for(int i=0; i < nfiles; i++)
		free(files[i]);
	free(files);

	// do not modify the main function after this point!
}
