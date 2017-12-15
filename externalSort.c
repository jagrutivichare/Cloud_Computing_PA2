#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>
#include <pthread.h>

int NUM_FILE_PARTITIONS  = 10;
long PARTITION_SIZE = 10;
int THREAD_COUNT=2;
int OUT_FILE_NUM=0;
int PROG_TYPE=1;
char **INPUT_FILES=NULL;
int FILE_COUNT=0;
struct arg_struct {
    int arg1;
    char **arg2;
};
pthread_mutex_t lock;
  
void clearFiles() {
    int i;
    for (i=0;i<OUT_FILE_NUM;i++) {
        char file[10]="";char str[5];
        sprintf(str, "%d", i);
        strcat(file, "out");
        strcat(file, str);
        strcat(file, ".txt");
        if(access(file,F_OK ) != -1 ) {
            remove(file);
        }      
    }
}

char *mergingFiles(char *file1,char *file2) {
    char *eof1;char *eof2;
    char *line1=(char*) malloc(100);
    char *line2=(char*) malloc(100);
    char *file=(char*) malloc(sizeof(char) * 10);            
    sprintf(file, "out%d.txt", OUT_FILE_NUM);
    FILE *fp3 = fopen(file,"w");
    FILE *fp1 = fopen(file1,"r");
    FILE *fp2 = fopen(file2,"r");
    OUT_FILE_NUM++;
    eof1=fgets(line1,100,fp1);
    eof2=fgets(line2,100,fp2);
    while (eof1 && eof2) {
        char* first=(char*) malloc(10);
        char* second=(char*) malloc(10);
        strncpy(first, line1, 10);
        strncpy(second, line2, 10);
        if (strcmp(first,second)<0 || strcmp(first,second)==0) {
            fprintf(fp3,"%s",line1);
            eof1=fgets(line1,100,fp1);
        } else {
            fprintf(fp3,"%s",line2);
            eof2=fgets(line2,100,fp2);
        }
    }
    if (eof1) {
        while (eof1) {
            fprintf(fp3,"%s",line1);
            eof1=fgets(line1,100,fp1);
        }
    } else {
        while (eof2) {
            fprintf(fp3,"%s",line2);
            eof2=fgets(line2,100,fp2);
        }
    }
    fclose(fp3);
    fclose(fp2);
    fclose(fp1);
    return file;
}

void mergeFiles(int partitions,char *in_files[]) {
    int i,j=0;
    int file_count = (partitions/2) + (partitions%2);
    char *Next_in_file[file_count];
    for (i=0;i<partitions;i+=2) {    
        if ((i+1)<partitions) {
            Next_in_file[j] = mergingFiles(in_files[i],in_files[i+1]);
        } else {
            Next_in_file[j]=in_files[i];
        }        
        j++;
    }
    j--;
    if (j > 0) {
        mergeFiles(j+1,Next_in_file);      
    } else {
        INPUT_FILES[FILE_COUNT]=Next_in_file[j];
        FILE_COUNT++;              
    }    
}

void *mergetwofiles(void *arg) { 
    pthread_mutex_lock(&lock);
    struct arg_struct *args = (struct arg_struct *)arg;
    mergeFiles(args -> arg1,args -> arg2);
    pthread_mutex_unlock(&lock);    
    pthread_exit(NULL);    
}

void mergeInputFiles() {
    int individual_array_size=NUM_FILE_PARTITIONS/THREAD_COUNT;
    char ***array = (char***)malloc((NUM_FILE_PARTITIONS) * sizeof(char**));
    struct arg_struct args[THREAD_COUNT];
    pthread_t thread_id[THREAD_COUNT];
    int thread[THREAD_COUNT]; int i;
    for (i=0;i<THREAD_COUNT;i++) {
        if(i==(THREAD_COUNT-1)) {
            individual_array_size=NUM_FILE_PARTITIONS-((NUM_FILE_PARTITIONS/THREAD_COUNT)*i);
        }
        array[i] = (char**)malloc(individual_array_size * sizeof(char*));
        int k;
        for(k = 0; k < individual_array_size; k++){
            array[i][k] = (char*) malloc(sizeof(char) * k * 10);
        }           
        int l,j=(NUM_FILE_PARTITIONS/THREAD_COUNT)*i;
        for(l=0;l<individual_array_size;l++) {
            array[i][l]=INPUT_FILES[j];
            j++;
        }
        args[i].arg1 = individual_array_size;
        args[i].arg2 = array[i];
        thread[i] = pthread_create(&(thread_id[i]),NULL,mergetwofiles,(void *)&args[i]);
    }        
    int k;
    for (k=0;k<THREAD_COUNT;k++) {
      pthread_join(thread_id[k], NULL);
    }
    pthread_mutex_destroy(&lock);
}

void generateOutput() {
    FILE *fp1=fopen(INPUT_FILES[FILE_COUNT-1], "r");
    char *file=(char*) malloc(sizeof(char) * 20);   
    if(PROG_TYPE == 1) {
        file="outputfile_128GB";
    } else if (PROG_TYPE == 2) {
        file="outfile_1TB";
    } else {
        file="outfile_1TB_8nodes";
    }
    FILE *fp2=fopen(file, "w");
    char *a=(char*) malloc(sizeof(char) * 100);
	if (fp2 != NULL) {
	    while (fgets(a, 100, fp1) != NULL && *a != '\n') {
		fputs(a, fp2);
	    }
	}
    printf("Final sorted records are in the file %s\n",file);
    fcloseall();
}

void mergeIntoOnefile() {
    char **FINAL_FILES = (char**)malloc(THREAD_COUNT * sizeof(char*));
    int k;
    for(k = 0; k < THREAD_COUNT; k++){
        FINAL_FILES[k] = (char*) malloc(sizeof(char) * k * 10);
    }
    int i;
    int j=FILE_COUNT-THREAD_COUNT;
    for (i=0;i<THREAD_COUNT;i++) {
        FINAL_FILES[i]=INPUT_FILES[j];
        j++;
    }
    mergeFiles(THREAD_COUNT,FINAL_FILES);
}

void merge(char *array[],int low,int mid,int high) {
    int low1,low2,i;char *temp[high];
    for(low1=low,low2=mid+1,i=low; low1 <= mid && low2 <= high; i++) {
        char* first=(char*) malloc(10);;
        char* second=(char*) malloc(10);;
        strncpy(first,array[low1],10);
        strncpy(second,array[low2],10);
        if(strcmp(first,second)<0 || strcmp(first,second)==0) {
            temp[i]=array[low1++];
        } else {
            temp[i]=array[low2++];
        }  
    }
    while(low1 <= mid)    
        temp[i++]=array[low1++];

    while(low2 <= high)   
        temp[i++]=array[low2++];

    for(i=low; i <= high; i++)
        array[i]=temp[i];
}

void implementMergeSort(char *array[],int l,int r){
    if (l<r) {
        int m=(l+r)/2;
        implementMergeSort(array,l,m);
        implementMergeSort(array,m+1,r);
        merge(array,l,m,r);
    }
}

void sortArrays(char *array[],int array_size) {
    implementMergeSort(array,0,array_size-1);
    char *file=(char*) malloc(sizeof(char) * 10);
    sprintf(file, "out%d.txt", OUT_FILE_NUM);
    OUT_FILE_NUM++;
    FILE *fpout = fopen(file, "w"); 
    int i;
    for(i=0;i<PARTITION_SIZE;i++){
        fprintf(fpout,"%s",array[i]);
    }   
    INPUT_FILES[FILE_COUNT]=file;
    FILE_COUNT++;
    fclose(fpout); 
}

void splitInputFile() {
    char *file = (char*) malloc(sizeof(char) * 10);
    if(PROG_TYPE == 1) {
        file="inputfile_128GB";
    } else {
        file="inputfile_1TB";
    }
    FILE *fp=fopen(file, "r");
    int i;
    char **input_array = (char**)malloc(PARTITION_SIZE * 100);
    for(i = 0; i < NUM_FILE_PARTITIONS; i++) {
        input_array[i] = (char*) malloc(100);
    }
    for (i=0;i<NUM_FILE_PARTITIONS;i++) {
        int j;
        for(j=0;j<PARTITION_SIZE;j++) {
            if(fgets(input_array[j],100,fp)==NULL) {
                break;
            }
        }
        sortArrays(input_array,j);       
    }
    fclose(fp);  
}

void initializeData() {
    if (pthread_mutex_init(&lock, NULL) != 0) {
        printf("mutex initialization failed\n");
        exit(0);
    }
    INPUT_FILES = (char**)malloc((NUM_FILE_PARTITIONS+THREAD_COUNT) * sizeof(char*));
    int i;
    for(i = 0; i < NUM_FILE_PARTITIONS; i++){
        INPUT_FILES[i] = (char*) malloc(sizeof(char) * i * 10);
    }
}

int main(int argc, char *argv[]) {
    int arg;
    while((arg=getopt(argc, argv, "n:p:t:s:")) != -1) {
        switch(arg) {
            case 'n':
            NUM_FILE_PARTITIONS=atoi(optarg);
            break;
            case 'p':
            PARTITION_SIZE=atoi(optarg);
            break;
            case 't':
            THREAD_COUNT=atoi(optarg);
            break;
            case 's':
            PROG_TYPE=atoi(optarg);
            break;
            default:
            break;
        }
    }

    initializeData();
    
    time_t begin,end;
    begin= time(NULL);
    
    splitInputFile();
    mergeInputFiles();
    mergeIntoOnefile();
    
    end = time(NULL);
    generateOutput();  
    if (PROG_TYPE == 1) {
        printf("Total time taken to sort 128GB data is %.6lf seconds\n",difftime(end, begin));
    } else if(PROG_TYPE == 1){
        printf("Total time taken to sort 1TB data is %.6lf seconds\n",difftime(end, begin));
    } else {
        printf("Total time taken to sort 1TB data on 8 nodes is %.6lf seconds\n",difftime(end, begin));
    }
    clearFiles();
}
