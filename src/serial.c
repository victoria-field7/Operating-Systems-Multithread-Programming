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




//Modidied function compress_directory for multiple threads:
//function starts here:
int compress_directory(char *directory_name) {
	DIR *d;
	struct dirent *dir;
	char **files = NULL;
	int nfiles = 0;
	int capacity = 0;

	d = opendir(directory_name);
	if(d == NULL) {
		printf("An error has occurred\n");
		return 0;
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
//function unfinished.... please continue here

	







	//Part of compress_directory function that was provided in the project (UNCHANGED CODE BELOW)
	// create a single zipped package with all text files in lexicographical order
	int total_in = 0, total_out = 0;
	FILE *f_out = fopen("text.tzip", "w");
	assert(f_out != NULL);
	int i = 0;
	for(i=0; i < nfiles; i++) {
		int len = strlen(directory_name)+strlen(files[i])+2;
		char *full_path = malloc(len*sizeof(char));
		assert(full_path != NULL);
		strcpy(full_path, directory_name);
		strcat(full_path, "/");
		strcat(full_path, files[i]);

		unsigned char buffer_in[BUFFER_SIZE];
		unsigned char buffer_out[BUFFER_SIZE];

		// load file
		FILE *f_in = fopen(full_path, "r");
		assert(f_in != NULL);
		int nbytes = fread(buffer_in, sizeof(unsigned char), BUFFER_SIZE, f_in);
		fclose(f_in);
		total_in += nbytes;

		// zip file
		z_stream strm;
		int ret = deflateInit(&strm, 9);
		assert(ret == Z_OK);
		strm.avail_in = nbytes;
		strm.next_in = buffer_in;
		strm.avail_out = BUFFER_SIZE;
		strm.next_out = buffer_out;

		ret = deflate(&strm, Z_FINISH);
		assert(ret == Z_STREAM_END);

		// dump zipped file
		int nbytes_zipped = BUFFER_SIZE-strm.avail_out;
		fwrite(&nbytes_zipped, sizeof(int), 1, f_out);
		fwrite(buffer_out, sizeof(unsigned char), nbytes_zipped, f_out);
		total_out += nbytes_zipped;

		free(full_path);
	}
	fclose(f_out);

	printf("Compression rate: %.2lf%%\n", 100.0*(total_in-total_out)/total_in);

	// release list of files
	for(i=0; i < nfiles; i++)
		free(files[i]);
	free(files);

	// do not modify the main function after this point!
	return 0;
}
