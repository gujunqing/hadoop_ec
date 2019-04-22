/*
Copyright (c) <2017>, Intel Corporation

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice,
this list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.
* Neither the name of Intel Corporation nor the names of its contributors
may be used to endorse or promote products derived from this software
without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/


#include "measurements.hpp"
#include "options.h"
#include "prealloc.h"
#include "random_number_generator.h"
#include "utils.hpp"
#include <algorithm>
#include <chrono>
#include <cstring>
#include <iostream>
#include <isa-l.h>

// Create the source data buffers with random data, plus extra room for the error correction codes.
// We create a total of 'm' buffers of lenght 'len'.
// The 'k' first buffers contain the data.
// The 'm-k' remaining buffer are left uninitialized, and will store the error correction codes.

int gfPow(int number,int t)
  {
    int result = 1;
    for(int i = 0; i < t; i++){
      result = gf_mul(result, number);
    }
    return result;
  }

void gf_gen_MSR_matrix(unsigned char*matrix, int n, int k)
{
    int r = n - k;
    int s = r;
    int m = n/s;
    int l = (int)pow(s, m);
    int i, j, e;
    int t = 0;
    unsigned int Ary[m];
	// memset(Ary, 0, m);
    int temp;
    int a, b;
    int u, v;
    unsigned char Gamma = 2;
    for(i=0; i < r; i++) {
      for (j=0; j < n; j++) {
        // the process of child matrix A(A,t)
        u = j%s;
        v = (j - u)/s + 1;
        for (a = 0; a < l; a++) {
          e = 0;
          temp = a;
          for (int ii = 0; ii < m; ii++) Ary[ii] = 0;
          while (temp/s != 0) {
            Ary[e] = temp % s;
            temp = temp / s;
            e++;
          }
          // convert the num o m bit s system Ary
          Ary[e] = temp % s;
          // decide the ele x comparing a_v with u
          int lambda = (v-1)*s+u+1;
          if (Ary[v-1] < u) {
            for (b=0; b < l; b++) {
              if (a == b)
                matrix[i*n*l*l + a*n*l + j*l +b] = gfPow(lambda, t);
              else
                matrix[i*n*l*l + a*n*l + j*l +b] = 0;
            }
          } else if (Ary[v-1] > u) {
            for (b=0; b < l; b++) {
              if (a == b)
                matrix[i*n*l*l + a*n*l + j*l + b] = gf_mul(Gamma, gfPow(lambda, t));
              else
                matrix[i*n*l*l + a*n*l + j*l + b] = 0;
            }
          } else {
            for (b=0; b < l; b++) {
              matrix[i*n*l*l + a*n*l + j*l + b] = 0;
            }
            // b = a(v,w), w=0,1,2,....,s-1
            for (int q=0; q < s; q++) {
              b = 0;
              Ary[v-1] = q;
              for (int p = 0; p < m; p++)
                b += pow(s, p)*Ary[p];
              lambda = (v-1)*s+1+q;
              matrix[i*n*l*l + a*n*l + j*l + b] = gfPow(lambda, t);
            }
          }
        }
      }
      t++;
    }
}

void gf_gen_MSR_encode_matrix(unsigned char *enocdeMatrix, unsigned char *MSRmatrix, int n, int k)
{
	int r = n - k;
    int s = r;
    int m = n/s;
    // assert(n%s == 0);
    int l = (int)pow(s, m);
    int i, j, e;
	unsigned char* tmpMatrix = (unsigned char*)malloc(r*l*r*l*sizeof(unsigned char));
	unsigned char* invertTmpMatrix = (unsigned char*)malloc(r*l*r*l*sizeof(unsigned char));
	memset(tmpMatrix, 0, r*l * r*l);
	memset(invertTmpMatrix, 0, r*l * r*l);

    for (i=0; i < r*l; i++)
      for (j=0; j < r*l; j++)
        tmpMatrix[i*r*l + j] = MSRmatrix[i*n*l + k*l + j];
    gf_invert_matrix(tmpMatrix, invertTmpMatrix, r*l);


    // temporarily do not need this one
    for (i=0; i < k*l; i++) {
      enocdeMatrix[i*k*l + i] = 1;
    }


    unsigned char temp;
    for (i=0; i < r*l; i++)
      for (j=0; j < k*l; j++) {
        temp = 0;
        for (e=0; e < r*l; e++)
          temp ^= gf_mul(invertTmpMatrix[i*r*l + e], MSRmatrix[e*n*l + j]);
        enocdeMatrix[k*l*k*l + i*k*l + j] = temp;
      }
	free(tmpMatrix);
	free(invertTmpMatrix);
}

uint8_t** create_source_data(int m, int k, int len)
{
    uint8_t** sources = (uint8_t**)malloc(m * sizeof(uint8_t*));

    random_number_generator<uint8_t> data_generator;

    for (int i = 0; i < m; ++i)
    {
        sources[i] = (uint8_t*)malloc(len * sizeof(uint8_t));
        if (i < k)
        {
            int j = 0;
            for (; j < len / 64 * 64; j += 64)
            {
                memset(&sources[i][j], data_generator.get(), 64);
            }
            for (; j < len; ++j)
            {
                sources[i][j] = (uint8_t)data_generator.get();
            }
        }
    }
    return sources;
}

uint8_t* LRC_RS_encode_data(int n,int k,int g,int l,int groups,uint8_t** sources,int len)
{
    // malloc encode matrix
    uint8_t* LRC_RS_encode_matrix = (uint8_t*)malloc(( k+g ) * k * sizeof(uint8_t));

    // malloc encode matrix table
    uint8_t* LRC_RS_encode_matrix_table = (uint8_t*)malloc(g * k * 32 * sizeof(uint8_t));

    // Generate encode matrix
    gf_gen_cauchy1_matrix(LRC_RS_encode_matrix, k+g, k);

    // Generates the expanded tables needed for fast encoding
    ec_init_tables(k, g, &LRC_RS_encode_matrix[k * k], LRC_RS_encode_matrix_table);

    // Actually generated the global parity
    ec_encode_data(
        len, k, g, LRC_RS_encode_matrix_table, (uint8_t**)sources, (uint8_t**)&sources[k]);

    // Actually generated the local parity 
    for(int i = 0; i < len; ++i)
        for(int j = 0; j < groups; j++){
        uint8_t s = 0;
            for(int p = 0; p < k/groups; p++){
                s ^= sources[p+j*k/groups][i];
            }
        sources[k+g+j][i] = s;
        }
 
    free(LRC_RS_encode_matrix_table);
    return LRC_RS_encode_matrix;
}

uint8_t* RS_encode_data(int n,int k, uint8_t** sources,int len)
{
    // malloc encode matrix
    uint8_t* RS_encode_matrix = (uint8_t*)malloc(n * k * sizeof(uint8_t));

    // malloc encode matrix table
    uint8_t* RS_encode_matrix_table = (uint8_t*)malloc((n-k) * k * 32 * sizeof(uint8_t));

    // Generate encode matrix
    gf_gen_cauchy1_matrix(RS_encode_matrix, n, k);

    // Generates the expanded tables needed for fast encoding
    ec_init_tables(k, n-k, &RS_encode_matrix[k * k], RS_encode_matrix_table);

    // Actually generated the global parity
    ec_encode_data(
        len, k, n-k, RS_encode_matrix_table, (uint8_t**)sources, (uint8_t**)&sources[k]);
 
    free(RS_encode_matrix_table);
    return RS_encode_matrix;
}

uint8_t* MSR_encode_data(int n,int k, uint8_t** sources,int len)
{
    // malloc encode matrix
    int r = n-k;
    int s = r;
    int m = n/s;
    int l = (int)pow(s, m);
    uint8_t* MSR_encode_matrix = (uint8_t*)malloc(n*l * k*l * sizeof(uint8_t));
    uint8_t* MSR_matrix = (uint8_t*)malloc(r*l * n*l * sizeof(uint8_t));
    memset(MSR_matrix, 0, r*l * n*l);
    memset(MSR_encode_matrix, 0, n*l * k*l);

    // malloc encode matrix table
    uint8_t* MSR_encode_matrix_table = (uint8_t*)malloc(n*l * k*l * 32 * sizeof(uint8_t));

    // Generate encode matrix
    gf_gen_MSR_matrix(MSR_matrix, n, k);
    gf_gen_MSR_encode_matrix(MSR_encode_matrix, MSR_matrix, n, k);

    // Generates the expanded tables needed for fast encoding
    ec_init_tables(k*l, r*l, &MSR_encode_matrix[k*l * k*l], MSR_encode_matrix_table);

    // Actually generated the global parity
    ec_encode_data(
        len, k*l, r*l, MSR_encode_matrix_table, (uint8_t**)sources, (uint8_t**)&sources[k*l]);
    
    free(MSR_matrix);
    free(MSR_encode_matrix_table);
    return MSR_encode_matrix;
}

// Create the error correction codes, and store them alongside the data.
std::vector<uint8_t> encode_data(int m, int k, uint8_t** sources, int len, prealloc_encode prealloc)
{
    // Generate encode matrix
    gf_gen_cauchy1_matrix(prealloc.encode_matrix.data(), m, k);

    // Generates the expanded tables needed for fast encoding
    ec_init_tables(k, m - k, &prealloc.encode_matrix[k * k], prealloc.table.data());

    // Actually generated the error correction codes
    ec_encode_data(
        len, k, m - k, prealloc.table.data(), (uint8_t**)sources, (uint8_t**)&sources[k]);

    return prealloc.encode_matrix;
}

// We randomly choose g+1 of the k original data lost in LRC code.
unsigned int* LRC_generate_errors(int k, int errors_count)
{
    random_number_generator<int> idx_generator(0, k - 1);
    unsigned int*  errors = (unsigned int*)malloc(errors_count * sizeof(unsigned int));
    int i = 0;
    int flag = 1;
    while(i < errors_count){
        errors[i] = idx_generator.get();
        for(int j = 0; j <i; j++){
            if(errors[j] == errors[i]){
                flag = 0;
            }
        }
        if(flag) {
            i++;
        }else{
            flag = 1;
        }
    }
    
    return errors;
}

// We arrange a new array of buffers that skip the ones we "lost"
uint8_t** LRC_create_erroneous_data(int k,int g, int groups,uint8_t** source_data, unsigned int* errors,int errors_count)
{
    uint8_t** erroneous_data;
    erroneous_data = (uint8_t**)malloc(k * sizeof(uint8_t*));
    int flag = 1;
    int num = 0;
    for(int i = 0; i < k+g; ++i){
        for(int j=0; j < errors_count; ++j){
            if(i == errors[j]) {flag = 0;num++;}
        }
        if(flag) erroneous_data[i-num] = source_data[i];
        flag = 1;
    }
    erroneous_data[k-1] = source_data[k+g+errors[0]/(k/groups)];
    return erroneous_data;
}

// Recover the contents of the "lost" buffers
// - k              : the number of buffer that contain the source data
// - g              : the number of buffer that contain the global parity
// - groups         : the groups of local parity(this test we aussume each group has only one local parity)
// - erroneous_data : the original buffers without the ones we "lost"
// - errors         : the indexes of the buffers we "lost"
// - errors_count   : the size of errors.
// - encode_matrix  : the matrix used to generate the error correction codes
// - len            : the length (in bytes) of each buffer
// Return the recovered "lost" buffers
uint8_t** LRC_recover_data(
    int                         k,
    int                         g,
    int                         groups,
    uint8_t**                   erroneous_data,
    unsigned int*               errors,
    int                         errors_count,
    uint8_t*                    encode_matrix,
    int                         len)
{
    uint8_t* LRC_decode_matrix = (uint8_t*)malloc(k*k*sizeof(uint8_t));
    uint8_t* LRC_decode_select_matrix = (uint8_t*)malloc(k*(g+1)*sizeof(uint8_t));
    uint8_t* LRC_invert_matrix = (uint8_t*)malloc(k*k*sizeof(uint8_t));

    uint8_t* LRC_decode_table = (uint8_t*)malloc(k*(g+1)*32*sizeof(uint8_t));

    uint8_t** LRC_decoding = (uint8_t**) malloc((g+1)*sizeof(uint8_t*));
    for (int i = 0; i < g+1; ++i)
    {
        LRC_decoding[i] = (uint8_t*)malloc(len * sizeof(uint8_t));
    }

    int flag = 1;
    int num = 0;
    for(int i = 0; i < k+g; ++i){
        for(int j=0; j < errors_count; ++j){
            if(i == errors[j]) {flag = 0;num++;}
        }
        if(flag) {
            for(int p = 0; p < k; ++p)
            LRC_decode_matrix[(i-num)*k + p] = encode_matrix[i*k+p];
        }
        flag = 1;
    }
    for(int i = 0; i < groups;++i)
        for(int j = 0; j < k/groups; ++j){
            if(i == errors[0]/(k/groups)){
                LRC_decode_matrix[(k-1)*k + i*k/groups + j] = 1;
            }else{
                LRC_decode_matrix[(k-1)*k + i*k/groups + j] = 0;
            }
        }

    gf_invert_matrix(LRC_decode_matrix, LRC_invert_matrix, k);
    
    for (int e = 0; e < errors_count; ++e)
    {
        int idx = errors[e];
        if (idx < k) // We lost one of the buffers containing the data
        {
            for (int j = 0; j < k; j++)
            {
                LRC_decode_select_matrix[k * e + j] = LRC_invert_matrix[k * idx + j];
            }
        }
        else // We lost one of the buffer containing the error correction codes
        {
            for (int i = 0; i < k; i++)
            {
                uint8_t s = 0;
                for (int j = 0; j < k; j++)
                    s ^= gf_mul(LRC_invert_matrix[j * k + i], encode_matrix[k * idx + j]);
                LRC_decode_select_matrix[k * e + i] = s;
            }
        }
    }


    ec_init_tables(k, g+1, LRC_decode_select_matrix, LRC_decode_table);
    ec_encode_data(len, k, g+1, LRC_decode_table, erroneous_data, LRC_decoding);

    return LRC_decoding;
}

// Recover the contents of the "lost" buffers
// - k              : the number of buffer that contain the source data
// - g              : the number of buffer that contain the global parity
// - groups         : the groups of local parity(this test we aussume each group has only one local parity)
// - erroneous_data : the original buffers without the ones we "lost"
// - errors         : the indexes of the buffers we "lost"
// - errors_count   : the size of errors.
// - encode_matrix  : the matrix used to generate the error correction codes
// - len            : the length (in bytes) of each buffer
// Return the recovered "lost" buffers
uint8_t** LRC_pcm_recover_data(
    int                         k,
    int                         g,
    int                         groups,
    uint8_t**                   erroneous_data,
    unsigned int*               errors,
    int                         errors_count,
    uint8_t*                    encode_matrix,
    int                         len)
{

    uint8_t* LRC_encode_extent_matrix = (uint8_t*)malloc((k+g+1)*k*sizeof(uint8_t));

    uint8_t* LRC_decode_matrix = (uint8_t*)malloc((g+1)*k*sizeof(uint8_t));

    uint8_t* LRC_decode_table = (uint8_t*)malloc((g+1)*k*32*sizeof(uint8_t));

    uint8_t** LRC_decoding = (uint8_t**) malloc((g+1)*sizeof(uint8_t*));
    for (int i = 0; i < g+1; ++i)
    {
        LRC_decoding[i] = (uint8_t*)malloc(len * sizeof(uint8_t));
    }

    for(int i = 0; i < k+g; ++i){
        for(int j=0; j < k; ++j){
            LRC_encode_extent_matrix[i*k+j] = encode_matrix[i*k+j];
        }
    }
    for(int i = 0; i < groups;++i)
        for(int j = 0; j < k/groups; ++j){
            if(i == errors[0]/(k/groups)){
                LRC_encode_extent_matrix[(k+g)*k + i*k/groups + j] = 1;
            }else{
                LRC_encode_extent_matrix[(k+g)*k + i*k/groups + j] = 0;
            }
        }

    gf_gen_PCM_matrix(LRC_encode_extent_matrix, LRC_decode_matrix, k+g+1, k, errors, errors_count);


    ec_init_tables(k, g+1, LRC_decode_matrix, LRC_decode_table);
    ec_encode_data(len, k, g+1, LRC_decode_table, erroneous_data, LRC_decoding);

    return LRC_decoding;
}

// We randomly choose r of the n data lost in RS code.
unsigned int* generate_errors(int n, int errors_count)
{
    random_number_generator<int> idx_generator(0, n - 1);
    unsigned int*  errors = (unsigned int*)malloc(errors_count * sizeof(unsigned int));
    int i = 0;
    int flag = 1;
    while(i < errors_count){
        errors[i] = idx_generator.get();
        for(int j = 0; j <i; j++){
            if(errors[j] == errors[i]){
                flag = 0;
            }
        }
        if(flag) {
            int k=0;
            while (k < i) {
                if (errors[k] > errors[i]){
                    int tmp = errors[i];
                    errors[i] = errors[k];
                    errors[k] = tmp;
                }
                k++;
            }
            i++;
        }else{
            flag = 1;
        }
    }
    return errors;
}

// We arrange a new array of buffers that skip the ones we "lost"
uint8_t** create_erroneous_data(int k, int r, uint8_t** source_data, unsigned int*errors, int errors_count)
{
    uint8_t** erroneous_data;
    erroneous_data = (uint8_t**)malloc(k * sizeof(uint8_t*));
    int flag = 1;
    int num = 0;
    for(int i = 0; i < k+r; ++i){
        for(int j=0; j < errors_count; ++j){
            if(i == errors[j]) {flag = 0;num++;}
        }
        if(flag) erroneous_data[i-num] = source_data[i];
        flag = 1;
    }
    return erroneous_data;
}

uint8_t** create_erroneous_data_MSR(int k, int r, uint8_t** source_data, unsigned int*errors, int errors_count)
{
    uint8_t** erroneous_data;
    int n = r+k;
    int s = r;
    int m = n / s;
    int l = (int)pow(s, m);
    int** dataIndexes = searchData((int*)errors, n, k, l, errors_count);
    erroneous_data = (uint8_t**)malloc(n * l * sizeof(uint8_t*));
    for (int p=0; p < n; p++) {
        for (int j=0; j < l; j++){
            if (dataIndexes[p][j] == 1){
                erroneous_data[p*l+j] = source_data[p*l+j];
            } else {
                erroneous_data[p*l+j] = NULL;
            }
        }
    }
    /*
    int flag = 1;
    int num = 0;
    for(int i = 0; i < n; ++i){
        for(int j=0; j < errors_count; ++j){
            if(i == errors[j]) {flag = 0;num++;}
        }
        for (int e=0; e<l; e++){
            if(flag)
                erroneous_data[i*l + e] = source_data[i*l + e];
            else
                erroneous_data[i*l + e] = NULL;
        }
        flag = 1;
    } */
    return erroneous_data;
}

// Recover the contents of the "lost" buffers
// - m              : the total number of buffer, containint both the source data and the error
//                    correction codes
// - k              : the number of buffer that contain the source data
// - erroneous_data : the original buffers without the ones we "lost"
// - errors         : the indexes of the buffers we "lost"
// - encode_matrix  : the matrix used to generate the error correction codes
// - len            : the length (in bytes) of each buffer
// Return the recovered "lost" buffers
uint8_t** recover_data_pcm(
    int                         m,
    int                         k,
    uint8_t**                   erroneous_data,
    unsigned int*               errors,
    int                         errors_count,
    uint8_t*                    encode_matrix,
    int                         len)
{
    uint8_t* decode_matrix = (uint8_t*)malloc((m-k)*k*sizeof(uint8_t));

    uint8_t* decode_table = (uint8_t*)malloc((m-k)*k*32*sizeof(uint8_t));

    uint8_t** decoding = (uint8_t**) malloc((m-k)*sizeof(uint8_t*));
    for (int i = 0; i < m-k; ++i)
    {
        decoding[i] = (uint8_t*)malloc(len * sizeof(uint8_t));
    }

	gf_gen_PCM_matrix(encode_matrix, decode_matrix, m, k, errors, errors_count);

    ec_init_tables(k, m - k, decode_matrix, decode_table);
    ec_encode_data(len, k, m - k, decode_table, erroneous_data, decoding);

    return decoding;
}

uint8_t** recover_data(
    int                         m,
    int                         k,
    uint8_t**                   erroneous_data,
    unsigned int*               errors,
    int                         errors_count,
    uint8_t*                    encode_matrix,
    int                         len)
{

    uint8_t* decode_matrix = (uint8_t*)malloc(k*k*sizeof(uint8_t));

    uint8_t* decode_select_matrix = (uint8_t*)malloc(k*(m-k)*sizeof(uint8_t));

    uint8_t* invert_matrix = (uint8_t*)malloc(k*k*sizeof(uint8_t));

    uint8_t* decode_table = (uint8_t*)malloc(k*(m-k)*32*sizeof(uint8_t));

    uint8_t** decoding = (uint8_t**) malloc((m-k)*sizeof(uint8_t*));
    for (int i = 0; i < m-k; ++i)
    {
        decoding[i] = (uint8_t*)malloc(len * sizeof(uint8_t));
    }

    int flag = 1;
    int num = 0;
    for(int i = 0; i < m; ++i){
        for(int j=0; j < errors_count; ++j){
            if(i == errors[j]) {flag = 0;num++;}
        }
        if(flag) {
            for(int p = 0; p < k; ++p)
            decode_matrix[(i-num)*k + p] = encode_matrix[i*k+p];
        }
        flag = 1;
    }
    
    gf_invert_matrix(decode_matrix, invert_matrix, k);
    
    for (int e = 0; e < errors_count; ++e)
    {
        int idx = errors[e];
        if (idx < k) // We lost one of the buffers containing the data
        {
            for (int j = 0; j < k; j++)
            {
                decode_select_matrix[k * e + j] = invert_matrix[k * idx + j];
            }
        }
        else // We lost one of the buffer containing the error correction codes
        {
            for (int i = 0; i < k; i++)
            {
                uint8_t s = 0;
                for (int j = 0; j < k; j++)
                    s ^= gf_mul(invert_matrix[j * k + i], encode_matrix[k * idx + j]);
                decode_select_matrix[k * e + i] = s;
            }
        }
    }

    ec_init_tables(k, m - k, decode_select_matrix, decode_table);
    ec_encode_data(len, k, (m - k), decode_table, erroneous_data, decoding);

    return decoding;
}

int* getValidIndexes(unsigned char** inputs, int n, int k) {
    int* validIndexes = (int*)malloc(k * sizeof(int));
    int idx = 0;
    for (int i = 0; i < n; i++) {
        if (inputs[i] != NULL) {
            validIndexes[idx++] = i;
        }
    }
    return validIndexes;
}

void mulPrepareDecoding(int n, int k, unsigned char** inputs, unsigned int* erasedIndexes, int errors_count, unsigned char* decodeMatrix)
{
    int r = n - k;
    int s = r;
    int m = n / s;
    int l = (int)pow(s, m);
    int i, j, p, q, t;
    // r = errors_count;
    // init the matrix
    uint8_t* tmpMatrix = (uint8_t*)malloc(k*l*k*l*sizeof(uint8_t));
    uint8_t* encodeMatrix = (uint8_t*)malloc(n * k * l * l * sizeof(uint8_t));
    uint8_t* invertMatrix = (uint8_t*)malloc(k * k * l * l * sizeof(uint8_t));
    // decodeMatrix = (uint8_t*)malloc(r * k * l * l * sizeof(uint8_t));
    uint8_t* MSR_matrix = (uint8_t*)malloc(r * n * l * l * sizeof(uint8_t));
    memset(MSR_matrix, 0, r*n*l*l);
    memset(tmpMatrix, 0, k*l*k*l);
    memset(encodeMatrix, 0, n*l*k*l);
    memset(invertMatrix, 0, k*l*k*l);

    // Generate encode matrix
    gf_gen_MSR_matrix(MSR_matrix, n, k);
    gf_gen_MSR_encode_matrix(encodeMatrix, MSR_matrix, n, k);

    // prepare the erasure data
    bool* erasureFlags = (bool*)malloc(n*l * sizeof(bool));
    int numErasedDataUnits = 0;

    for (i = 0; i < errors_count; i++) {
        int index =erasedIndexes[i];
        for (j=0; j < l; j++) {
            erasureFlags[index * l + j] = true;
            if (index < k) {
                numErasedDataUnits++;
            }
        }
    }
    
    // assert numErasedDataUnits/l == errors_count;

    // find valid input k*l size
    int* validIndexes = getValidIndexes(inputs, n*l, k*l);

    for (i = 0; i < k*l; i++) {
        t = validIndexes[i];
        for (j = 0; j < k*l; j++) {
            tmpMatrix[k*l * i + j] =
                    encodeMatrix[k*l * t + j];
        }
    }

    gf_invert_matrix(tmpMatrix, invertMatrix, k*l);

    for (i = 0; i < numErasedDataUnits/l; i++) {
        for (q = 0; q < l; q++) {
            for (j = 0; j < k*l; j++) {
                decodeMatrix[k*l * (i*l+q) + j] =
                        invertMatrix[k*l * (erasedIndexes[i]*l+q) + j];
            }
        }
    }

    for (p = numErasedDataUnits/l; p < errors_count; p++) {
        for (q = 0; q < l; q++) {
            for (i = 0; i < k*l; i++) {
                unsigned char ttmp = 0;
                for (j = 0; j < k*l; j++) {
                    ttmp ^= gf_mul(invertMatrix[j * k*l + i],
                            encodeMatrix[k*l * (erasedIndexes[p]*l+q) + j]);
                }
                decodeMatrix[k*l * (p*l+q) + i] = ttmp;
            }
        }
    }

    free(MSR_matrix);
    free(tmpMatrix);
    free(encodeMatrix);
    free(invertMatrix);
    free(validIndexes);
}

void mulPrepareAPCMDecoding(int n, int k, unsigned char** inputs, unsigned int* erasedIndexes, int errors_count, unsigned char* decodeMatrix)
{
    int r = n - k;
    int s = r;
    int m = n / s;
    int l = (int)pow(s, m);
    int i, j, p, q;
    r = errors_count;
    int rr = n-k-r;
    unsigned char tar, inv, tmp;

    // decodeMatrix = (uint8_t*)malloc(r * k * l * l * sizeof(uint8_t));
    uint8_t* MSRMatrix = (uint8_t*)malloc(r * n * l * l * sizeof(uint8_t));
    memset(MSRMatrix, 0, r * n * l * l);
    gf_gen_MSR_matrix(MSRMatrix, n, k);

    // find valid input k*l size
    int* validIndexes = getValidIndexes(inputs, n*l, k*l);

    // DumpUtil.dumpMatrix(MSRMatrix, r*l, n*l);
    for (i=0; i < r; i++) {
        int idx = erasedIndexes[i];
        for (j=0; j < l; j++){
            tar = MSRMatrix[(i*l+j) * n*l + idx*l+j];
            if (tar != 1){
                inv = gf_inv(tar);
                for (p = 0; p < (n-rr)*l; p++)
                    MSRMatrix[(i*l+j) * l*n + p] = gf_mul(MSRMatrix[(i*l+j) * l*n + p], inv);
            }
            for (p = 0; p < r*l; p++) {
                if (p == (i*l+j))  continue;
                tmp = MSRMatrix[p * l*n + idx*l+j];
                for (q = 0; q < (n-rr)*l; q++)
                    MSRMatrix[p * l*n + q] ^= gf_mul(MSRMatrix[(i*l+j) * l*n + q], tmp);
            }
        }
    }
    // Divide the PCM matrix
    int pos, t;
    for (i = 0; i < r*l; i++) {
        pos = i*n*l;
        for (j = 0; j < k*l; j++) {
            t = validIndexes[j];
            decodeMatrix[k*l * i + j] = MSRMatrix[pos + t];
        }
    }
    free(MSRMatrix);
}

void singlePrepareAPCMDecoding(int n, int k, unsigned char** inputs, unsigned int* erasedIndexes, int errors_count, unsigned char* decodeMatrix) 
{
    int i, j, p, q, e, w;
    int v, u;
    int a, b, temp;
    int r = n - k;
    int s = r;
    int m = n / s;
    int l = (int)pow(s, m);
    int errorTip = erasedIndexes[0];
    unsigned char tar, inv, tmp;

    uint8_t* MSRMatrix = (uint8_t*)malloc(r * n * l * l * sizeof(uint8_t));
    memset(MSRMatrix, 0, r * n * l * l);
    gf_gen_MSR_matrix(MSRMatrix, n, k);

    // find valid input k*l size

    // initialize
    int Ary[m];
    int a_index[l/s];
    u = errorTip % s;
    v = (errorTip-u)/s + 1;
    uint8_t* leftMatrix = (uint8_t*)malloc(l*l * sizeof(uint8_t));
    uint8_t* invertleftMatrix = (uint8_t*)malloc(l*l * sizeof(uint8_t));
    uint8_t* tmpMatrix = (uint8_t*)malloc(l*(n-1)*l/r * sizeof(uint8_t));
    // decodeMatrix = (uint8_t*)malloc(l*(n-1)*l/r * sizeof(uint8_t));

    // a_v = u
    w = 0;
    for (a=0; a < l; a++) {
        e = 0;
        temp = a;
        for (p=0; p < m; p++) Ary[p] = 0;
        while (temp/s != 0) {
            Ary[e] = temp%s;
            temp = temp/s;
            e++;
        }
        Ary[e] = temp % s;
        if (Ary[v-1] == u) {
            a_index[w] = a;
            w++;
        }
    }

    // set the decode matrix valuye
    for (j=0; j < r; j++)
        for (a=0; a < l/s; a++){
            for (b=0; b < l; b++)
                leftMatrix[j*(l/s)*l + a*l + b] = MSRMatrix[j*l*n*l + a_index[a]*n*l + errorTip*l + b];
            int pp=0;
            for (p=0; p < n; p++){
                if (p == errorTip) {
                    continue;
                }
                for(b=0; b < l/r; b++){
                    tmpMatrix[j*l/s*(n-1)*l/s + a*(n-1)*l/s + pp*l/r + b] =
                            MSRMatrix[j*l*n*l + a_index[a]*n*l + p*l + a_index[b]];
                }
                pp++;
            }
        }
    // DumpUtil.dumpMatrix(tmpMatrix, l, (n-1)*l/r);
    gf_invert_matrix(leftMatrix, invertleftMatrix, l);
    // generate decode matrix
    for (i=0; i < l; i++)
        for (j=0; j < (n-1)*l/r; j++) {
            tmp = 0;
            for (e=0; e < l; e++)
                tmp ^= gf_mul(invertleftMatrix[i*l + e], tmpMatrix[e*(n-1)*l/r + j]);
            decodeMatrix[i*(n-1)*l/r + j] = tmp;
        }
    free(leftMatrix);
    free(invertleftMatrix);
    free(tmpMatrix);
}

uint8_t** recover_data_MSR(
        int                         n,
        int                         k,
        uint8_t**                   erroneous_data,
        unsigned int*               errors,
        int                         errors_count,
        uint8_t*                    encode_matrix,
        int                         len,
        bool                        isPcm)
{
    int r = n-k;
    int s = r;
    int m = n / s;
    int l = (int)pow(s, m);

    uint8_t** decoding = (uint8_t**) malloc(errors_count*l*sizeof(uint8_t*));
    for (int i = 0; i < errors_count*l; ++i)
    {
        decoding[i] = (uint8_t*)malloc(len * sizeof(uint8_t));
    }
    uint8_t* decode_matrix;
    uint8_t* decode_table;
    uint8_t** realInputs;
    int* validIndexes;
    

    // gf_gen_PCM_matrix(encode_matrix, decode_matrix, n, k, errors, errors_count);
    if (!isPcm){
        if (errors_count == 1)
        {
            std::cout << "The normal method do not support single recover right now!!!" << std::endl; 
            exit(1);
        }
        decode_matrix = (uint8_t*)malloc(errors_count*k*l*l*sizeof(uint8_t));
        memset(decode_matrix, 0, errors_count*k*l*l);
        decode_table = (uint8_t*)malloc(errors_count*k*l*l*32*sizeof(uint8_t));
        mulPrepareDecoding(n, k, erroneous_data, errors, errors_count, decode_matrix);
        realInputs = (uint8_t**)malloc(k*l * sizeof(uint8_t*));
        validIndexes = getValidIndexes(erroneous_data, n*l, k*l);
        for (int i = 0; i < k*l; i++)
            realInputs[i] = erroneous_data[validIndexes[i]];
        
        ec_init_tables(k*l, errors_count*l, decode_matrix, decode_table);
        ec_encode_data(len, k*l, errors_count*l, decode_table, realInputs, decoding);
    } else {
        if (errors_count == 1) {
            decode_matrix = (uint8_t*)malloc(l*(n-1)*l/r * sizeof(uint8_t));
            memset(decode_matrix, 0, l*(n-1)*l/r);
            decode_table = (uint8_t*)malloc(l*(n-1)*l/r * 32 * sizeof(uint8_t));
            singlePrepareAPCMDecoding(n, k, erroneous_data, errors, errors_count, decode_matrix);
            realInputs = (uint8_t**)malloc(k*l * sizeof(uint8_t*));
            validIndexes = getValidIndexes(erroneous_data, n*l, (n-1)*l/r);
            // int* validIndexes = getValidIndexes(inputs, n*l, (n-1)*l/r);
            for (int i = 0; i < (n-1)*l/r; i++)
                realInputs[i] = erroneous_data[validIndexes[i]];
            ec_init_tables((n-1)*l/r, l, decode_matrix, decode_table);
            ec_encode_data(len, (n-1)*l/r, l, decode_table, realInputs, decoding);
        }
        else {
            decode_matrix = (uint8_t*)malloc(errors_count*k*l*l*sizeof(uint8_t));
            memset(decode_matrix, 0, errors_count*k*l*l);
            decode_table = (uint8_t*)malloc(errors_count*k*l*l*32*sizeof(uint8_t));

            mulPrepareAPCMDecoding(n, k, erroneous_data, errors, errors_count, decode_matrix);
            realInputs = (uint8_t**)malloc(k*l * sizeof(uint8_t*));
            validIndexes = getValidIndexes(erroneous_data, n*l, k*l);
            for (int i = 0; i < k*l; i++)
                realInputs[i] = erroneous_data[validIndexes[i]];
    
            ec_init_tables(k*l, errors_count*l, decode_matrix, decode_table);
            ec_encode_data(len, k*l, errors_count*l, decode_table, realInputs, decoding);
        }
    }
    /*
    uint8_t** realInputs = (uint8_t**)malloc(k*l * sizeof(uint8_t*));
    int* validIndexes = getValidIndexes(erroneous_data, n*l, k*l);
    for (int i = 0; i < k*l; i++)
        realInputs[i] = erroneous_data[validIndexes[i]];
    
    ec_init_tables(k*l, errors_count*l, decode_matrix, decode_table);
    ec_encode_data(len, k*l, errors_count*l, decode_table, realInputs, decoding);
    */
    free(decode_table);
    free(decode_matrix);
    free(realInputs);
    free(validIndexes);

    return decoding;
}


int main_RS(int argc, char* argv[])
{
    using namespace std::chrono_literals;

    options options = options::parse(argc, argv);

    //utils::display_info(options);

    //
    int n;
    int k            = 8;
    int r            = 2;
    int len          = 1024ul;
    int errors_count = r;
    n = k + r;

    std::chrono::nanoseconds time = 0s;
    std::chrono::nanoseconds pcm_time = 0s;
    int circle = 1000;
    bool success = false;

    for(int count = 0; count < circle ;++count){
    uint8_t** source_data = create_source_data(n, k, len);

    // Decoding and encoding process with RS method.

    //encode data

    uint8_t * RSmatrix;

    RSmatrix = RS_encode_data(n, k, source_data, len);

    //make errors
    unsigned int* errors;
    errors = generate_errors(n,errors_count);

    //decode data with RS method

    //create erroneous data
    uint8_t** erroneous_data;
    erroneous_data = create_erroneous_data(k, r , source_data, errors, errors_count);

    //generate decode matrix and recover
    auto                 start_decode = std::chrono::steady_clock::now();
    uint8_t** decoding;
    decoding = recover_data(n, k, erroneous_data, errors, errors_count, RSmatrix, len);
    
    auto                 end_decode = std::chrono::steady_clock::now();

    time += end_decode - start_decode;
    //check 

    success = false;
    for (int i = 0; i < errors_count; ++i)
    {
        int ret = memcmp(source_data[errors[i]], decoding[i], len);

        if(ret == 0) success = true;
        else success = false;
    }
    

    //decode data with pcm method

    //generate decode matrix and recover
    auto                 start_decode_pcm = std::chrono::steady_clock::now();
    uint8_t** decoding_pcm;
    decoding_pcm = recover_data_pcm(n, k, erroneous_data, errors, errors_count, RSmatrix, len);

    auto                 end_decode_pcm = std::chrono::steady_clock::now();
    pcm_time += end_decode_pcm - start_decode_pcm;
     //check 

    success = false;
    for (int i = 0; i < errors_count; ++i)
    {
        int ret = memcmp(source_data[errors[i]], decoding_pcm[i], len);

        if(ret == 0) success = true;
        else success = false;
    }
    

    free(RSmatrix);
    free(errors);
    free(erroneous_data);

    for (int i = 0; i < n; ++i)
    {
        free(source_data[i]);
    }
    free(source_data);

    for (int i = 0; i < errors_count; ++i)
    {
        free(decoding[i]);
    }
    free(decoding);

    for (int i = 0; i < errors_count; ++i)
    {
        free(decoding_pcm[i]);
    }
    free(decoding_pcm);


    }

    std::cout << "\n";

    if(success) std::cout << "success";
    else std::cout << "fail";

    std::cout << "\n";

    std::cout << "[Info   ] Recover time:                "
              << utils::duration_to_string(time/circle) << "\n";
    std::cout << "\n";

    std::cout << "[Info   ] Recover time:                "
              << utils::duration_to_string(pcm_time/circle) << "\n";
    std::cout << "\n";

    if(success) std::cout << "pcm recover success";
    else std::cout << "pcm recover fail";
    std::cout << "\n";
}

int main_MSR(int argc, char* argv[])
{
    using namespace std::chrono_literals;

    options options = options::parse(argc, argv);

    //utils::display_info(options);

    //
    int n;
    int k            = 6;
    int r            = 2;
    int len          = 1024ul;
    int errors_count = r;
    n = k + r;
    int s = r;
    int m = n / s;
    int l = (int)pow(s, m);
    std::chrono::nanoseconds time = 0s;
    std::chrono::nanoseconds pcm_time = 0s;
    int circle = 1024*1024/l;
    bool success = false;

    for(int count = 0; count < circle ;++count){
        uint8_t** source_data = create_source_data(n*l, k*l, len);

        // Decoding and encoding process with RS method.

        //encode data

        uint8_t * RSmatrix;

        RSmatrix = MSR_encode_data(n, k, source_data, len);

        //make errors
        unsigned int* errors;
        errors = generate_errors(n, errors_count);

        //decode data with MSR method

        //create erroneous data
        uint8_t** erroneous_data;
        erroneous_data = create_erroneous_data_MSR(k, r , source_data, errors, errors_count);

        //generate decode matrix and recover
        auto                 start_decode = std::chrono::steady_clock::now();
        uint8_t** decoding;
        decoding = recover_data_MSR(n, k, erroneous_data, errors, errors_count, RSmatrix, len, false);
        
        auto                 end_decode = std::chrono::steady_clock::now();

        time += end_decode - start_decode;
        //check 

        success = false;
        for (int i = 0; i < errors_count; ++i)
        {
            for (int j=0; j<l; j++){
                int ret = memcmp(source_data[errors[i]*l+j], decoding[i*l+j], len);
                if(ret == 0) success = true;
                else success = false;
            }
        }
        

        //decode data with pcm method

        //generate decode matrix and recover
        auto                 start_decode_pcm = std::chrono::steady_clock::now();
        uint8_t** decoding_pcm;
        decoding_pcm = recover_data_MSR(n, k, erroneous_data, errors, errors_count, RSmatrix, len, true);

        auto                 end_decode_pcm = std::chrono::steady_clock::now();
        pcm_time += end_decode_pcm - start_decode_pcm;
        //check 

        success = false;
        for (int i = 0; i < errors_count; ++i)
        {
            for (int j=0; j<l; j++){
                int ret = memcmp(source_data[errors[i]*l+j], decoding[i*l+j], len);
                if(ret == 0) success = true;
                else success = false;
            }
        }
        

        free(RSmatrix);
        free(errors);
        free(erroneous_data);

        for (int i = 0; i < n*l; ++i)
        {
            free(source_data[i]);
        }
        free(source_data);

        for (int i = 0; i < errors_count*l; ++i)
        {
            free(decoding[i]);
        }
        free(decoding);

        for (int i = 0; i < errors_count*l; ++i)
        {
            free(decoding_pcm[i]);
        }
        free(decoding_pcm);


    }

    std::cout << "\n";

    if(success) std::cout << "success";
    else std::cout << "fail";

    std::cout << "\n";

    std::cout << "[Info   ] Recover time:                "
              << utils::duration_to_string(time) << "\n";
    std::cout << "\n";

    std::cout << "[Info   ] Recover time:                "
              << utils::duration_to_string(pcm_time) << "\n";
    std::cout << "\n";

    if(success) std::cout << "pcm recover success";
    else std::cout << "pcm recover fail";
    std::cout << "\n";
}

int main(int argc, char* argv[])
{
    using namespace std::chrono_literals;

    options options = options::parse(argc, argv);

    //utils::display_info(options);

    //
    int n;
    int k            = 4;
    int r            = 2;
    int len          = 1024ul;
    int errors_count = 1;
    n = k + r;
    int s = r;
    int m = n / s;
    int l = (int)pow(s, m);
    std::chrono::nanoseconds pcm_time = 0s;
    int circle = 1;
    bool success = false;

    for(int count = 0; count < circle ;++count){
        uint8_t** source_data = create_source_data(n*l, k*l, len);

        // Decoding and encoding process with RS method.

        //encode data

        uint8_t * RSmatrix;

        RSmatrix = MSR_encode_data(n, k, source_data, len);

        //make errors
        unsigned int* errors;
        errors = generate_errors(n, errors_count);

        //decode data with MSR method

        //create erroneous data
        uint8_t** erroneous_data;
        erroneous_data = create_erroneous_data_MSR(k, r , source_data, errors, errors_count);
        //decode data with pcm method

        //generate decode matrix and recover
        auto                 start_decode_pcm = std::chrono::steady_clock::now();
        uint8_t** decoding_pcm;
        decoding_pcm = recover_data_MSR(n, k, erroneous_data, errors, errors_count, RSmatrix, len, true);

        auto                 end_decode_pcm = std::chrono::steady_clock::now();
        pcm_time += end_decode_pcm - start_decode_pcm;
        //check 

        success = false;
        for (int i = 0; i < errors_count; ++i)
        {
            for (int j=0; j<l; j++){
                int ret = memcmp(source_data[errors[i]*l+j], decoding_pcm[i*l+j], len);
                if(ret == 0) success = true;
                else success = false;
            }
        }
        free(RSmatrix);
        free(errors);
        free(erroneous_data);

        for (int i = 0; i < n*l; ++i)
        {
            free(source_data[i]);
        }
        free(source_data);
        for (int i = 0; i < errors_count*l; ++i)
        {
            free(decoding_pcm[i]);
        }
        free(decoding_pcm);


    }
    std::cout << "\n";

    std::cout << "[Info   ] Recover time:                "
              << utils::duration_to_string(pcm_time) << "\n";
    std::cout << "\n";

    if(success) std::cout << "pcm recover success";
    else std::cout << "pcm recover fail";
    std::cout << "\n";
}
