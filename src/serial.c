#include <dirent.h>
#include <pthread.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <zlib.h>   // allowed library to compress the files
#define BUFFER_SIZE 1048576 // 1MB
#define MAX_THREADS 19 // We should keep threads under 20 for this project */

/*Group #: 25
Group Members (with NetIDs):
Victoria Field, victoriafield
Armani Romay, armaniromay
Jesse Herrera, herrera42
Jawaad Ramcharitar, jawaadramcharitar
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

//This function builds the full path of the file
static void build_full_path(const char *directory_name, const char *file_name, char *buffer, size_t buffer_size) {
	size_t dir_len = strlen(directory_name);
	int needs_slash = (dir_len > 0 && directory_name[dir_len - 1] == '/') ? 0 : 1;

	if(needs_slash)
		snprintf(buffer, buffer_size, "%s/%s", directory_name, file_name);
	else
		snprintf(buffer, buffer_size, "%s%s", directory_name, file_name);
}

// Worker thread: pull next file index, compress it entirely into memory,
// and store the result in the pre-allocated results array at the same index.
void *compress_worker(void *arg) {
	worker_context_t *ctx = (worker_context_t *) arg;

	for(;;) {
		int file_index;

		// critical section: atomically fetch next file index from shared counter
		pthread_mutex_lock(&ctx->lock);
		if(ctx->next_index >= ctx->nfiles) {
			// no more files to process, exit the thread
			pthread_mutex_unlock(&ctx->lock);
			break;
		}
		file_index = ctx->next_index;
		ctx->next_index++;
		pthread_mutex_unlock(&ctx->lock);

		// build full path
		char full_path[1024];
		build_full_path(ctx->directory_name, ctx->files[file_index], full_path, sizeof(full_path));

		// open file and get size
		FILE *f_in = fopen(full_path, "rb");
		assert(f_in != NULL);
		
		// get file size using stat (faster than fseek/ftell)
		struct stat st;
		int stat_ret = fstat(fileno(f_in), &st);
		assert(stat_ret == 0);
		size_t file_size = (size_t) st.st_size;
		
		// allocate buffer and read entire file in one operation
		unsigned char *input_buffer = (unsigned char *) malloc(file_size);
		assert(input_buffer != NULL || file_size == 0);
		if(file_size > 0) {
			size_t read_count = fread(input_buffer, 1, file_size, f_in);
			assert(read_count == file_size);
		}
		fclose(f_in);

		// allocate output buffer with sufficient capacity
		uLongf bound = compressBound((uLong) file_size);
		unsigned char *output_buffer = (unsigned char *) malloc((size_t) bound);
		assert(output_buffer != NULL || bound == 0);

		// compress
		z_stream strm;
		memset(&strm, 0, sizeof(strm));
		int ret = deflateInit(&strm, 9);
		assert(ret == Z_OK);

		strm.next_in = input_buffer;
		strm.avail_in = (uInt) file_size;
		strm.next_out = output_buffer;
		strm.avail_out = (uInt) bound;

		ret = deflate(&strm, Z_FINISH);
		assert(ret == Z_STREAM_END);
		deflateEnd(&strm);

		int compressed_size = (int)((size_t)bound - (size_t)strm.avail_out);

		// store result
		ctx->results[file_index].data = output_buffer;
		ctx->results[file_index].compressed_size = compressed_size;
		ctx->results[file_index].original_size = (int) file_size;

		// free input
		free(input_buffer);
	}

	pthread_exit(NULL);
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
	// create a single zipped package with all text files in lexicographical order
	int total_in = 0, total_out = 0;

	// initialize worker context
	worker_context_t ctx;
	ctx.files = files;
	ctx.nfiles = nfiles;
	ctx.directory_name = directory_name;
	ctx.next_index = 0;
	ctx.results = (compressed_result_t *) calloc((size_t) nfiles, sizeof(compressed_result_t));
	assert(ctx.results != NULL || nfiles == 0);
	pthread_mutex_init(&ctx.lock, NULL);

	// spin up at most MAX_THREADS threads
	int num_threads = nfiles < MAX_THREADS ? nfiles : MAX_THREADS;
	pthread_t threads[MAX_THREADS];
	for(int t = 0; t < num_threads; t++) {
		int ret = pthread_create(&threads[t], NULL, compress_worker, &ctx);
		assert(ret == 0);
	}
	for(int t = 0; t < num_threads; t++) {
		int ret = pthread_join(threads[t], NULL);
		assert(ret == 0);
	}

	// write results in lex order to ensure correct output layout
	FILE *f_out = fopen("text.tzip", "wb");
	assert(f_out != NULL);
	
	// use larger buffer for file output to reduce system calls
	setvbuf(f_out, NULL, _IOFBF, BUFFER_SIZE);
	
	for(int i = 0; i < nfiles; i++) {
		int nbytes_zipped = ctx.results[i].compressed_size;
		fwrite(&nbytes_zipped, sizeof(int), 1, f_out);
		if(nbytes_zipped > 0) {
			fwrite(ctx.results[i].data, 1, (size_t) nbytes_zipped, f_out);
		}
		total_in += ctx.results[i].original_size;
		total_out += nbytes_zipped;
		free(ctx.results[i].data);
	}
	fclose(f_out);

	pthread_mutex_destroy(&ctx.lock);
	free(ctx.results);

	printf("Compression rate: %.2lf%%\n", total_in == 0 ? 0.0 : 100.0*(total_in-total_out)/total_in);

	// release list of files
	for(int i = 0; i < nfiles; i++)
		free(files[i]);
	free(files);
}
