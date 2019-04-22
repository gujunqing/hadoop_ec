/**********************************************************************
  Copyright(c) 2011-2015 Intel Corporation All rights reserved.

  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions
  are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in
      the documentation and/or other materials provided with the
      distribution.
    * Neither the name of Intel Corporation nor the names of its
      contributors may be used to endorse or promote products derived
      from this software without specific prior written permission.

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
  A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
  OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
  THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
**********************************************************************/

#include <limits.h>
#include <string.h>		// for memset
#include <math.h>
#include <stdlib.h>
#include "erasure_code.h"
#include "ec_base.h"		// for GF tables
#include "types.h"

void ec_init_tables(int k, int rows, unsigned char *a, unsigned char *g_tbls)
{
	int i, j;

	for (i = 0; i < rows; i++) {
		for (j = 0; j < k; j++) {
			gf_vect_mul_init(*a++, g_tbls);
			g_tbls += 32;
		}
	}
}

unsigned char gf_mul(unsigned char a, unsigned char b)
{
#ifndef GF_LARGE_TABLES
	int i;

	if ((a == 0) || (b == 0))
		return 0;

	return gff_base[(i = gflog_base[a] + gflog_base[b]) > 254 ? i - 255 : i];
#else
	return gf_mul_table_base[b * 256 + a];
#endif
}

unsigned char gf_inv(unsigned char a)
{
#ifndef GF_LARGE_TABLES
	if (a == 0)
		return 0;

	return gff_base[255 - gflog_base[a]];
#else
	return gf_inv_table_base[a];
#endif
}

void gf_gen_rs_matrix(unsigned char *a, int m, int k)
{
	int i, j;
	unsigned char p, gen = 1;

	memset(a, 0, k * m);
	for (i = 0; i < k; i++)
		a[k * i + i] = 1;

	for (i = k; i < m; i++) {
		p = 1;
		for (j = 0; j < k; j++) {
			a[k * i + j] = p;
			p = gf_mul(p, gen);
		}
		gen = gf_mul(gen, 2);
	}
}

void gf_gen_cauchy1_matrix(unsigned char *a, int m, int k)
{
	int i, j;
	unsigned char *p;

	// Identity matrix in high position
	memset(a, 0, k * m);
	for (i = 0; i < k; i++)
		a[k * i + i] = 1;

	// For the rest choose 1/(i + j) | i != j
	p = &a[k * k];
	for (i = k; i < m; i++)
		for (j = 0; j < k; j++)
			*p++ = gf_inv(i ^ j);
}

void gf_gen_PCM_matrix(unsigned char *src, unsigned char *dest, int n, int k, unsigned int *error, int errors_count)
{
    int i, j, e;
	int r = n-k;
	int rr = n-k-r;
    // Initial matrix

    // unsigned char* free = (unsigned char *)malloc(((m - k) * k) * sizeof(unsigned char*));
    unsigned char* PCM = (unsigned char *)malloc((errors_count * n) * sizeof(unsigned char*));

    memset(dest, 0, errors_count * k);
    memset(PCM, 0, errors_count * n);

    for(i = 0; i < errors_count; ++i)
        for(j = 0; j < k;++j)
            PCM[i * n + j] = src[(i+k) * k + j];

    for(i = 0; i < errors_count; ++i)
        PCM[i * n + k + i] = 1;

    for (i=0; i<errors_count; i++){
        int idx = error[i];
        int a = PCM[i*n+idx];
        int inv_a, tmp_a;
        if (a != 1) {
            inv_a = gf_inv(a);
            for (j = 0; j < n-rr; j++) {
                PCM[i * n + j] = gf_mul(PCM[i * n + j], inv_a);
            }
        }
        for (j=0; j < errors_count; j++){
            if (j == i) continue;
            // else if (PCM[j * m + idx] == 0) continue;
            tmp_a = PCM[j * n + idx];
            for (e=0; e < n-rr; e++){
                PCM[j * n + e] ^= gf_mul(PCM[i * n + e], tmp_a);
            }
        }
    }

	// divide the PCM matrix
	int valid[k];
    int p=0, t;
    int pos = 0;
    for (i=0; i<n-rr;i++) {
    	if (p < errors_count && error[p] == i) {
			p++;
			continue;
		}
        valid[pos++] = i;
    }
    for (i = 0; i < errors_count; i++) {
      	for (j = 0; j < k; j++) {
        	t = valid[j];
        	dest[k * i + j] = PCM[i*n + t];
      	}
    }
	free(PCM);
}

int gf_invert_matrix(unsigned char *in_mat, unsigned char *out_mat, const int n)
{
	int i, j, k;
	unsigned char temp;

	// Set out_mat[] to the identity matrix
	for (i = 0; i < n * n; i++)	// memset(out_mat, 0, n*n)
		out_mat[i] = 0;

	for (i = 0; i < n; i++)
		out_mat[i * n + i] = 1;

	// Inverse
	for (i = 0; i < n; i++) {
		// Check for 0 in pivot element
		if (in_mat[i * n + i] == 0) {
			// Find a row with non-zero in current column and swap
			for (j = i + 1; j < n; j++)
				if (in_mat[j * n + i])
					break;

			if (j == n)	// Couldn't find means it's singular
				return -1;

			for (k = 0; k < n; k++) {	// Swap rows i,j
				temp = in_mat[i * n + k];
				in_mat[i * n + k] = in_mat[j * n + k];
				in_mat[j * n + k] = temp;

				temp = out_mat[i * n + k];
				out_mat[i * n + k] = out_mat[j * n + k];
				out_mat[j * n + k] = temp;
			}
		}

		temp = gf_inv(in_mat[i * n + i]);	// 1/pivot
		for (j = 0; j < n; j++) {	// Scale row i by 1/pivot
			in_mat[i * n + j] = gf_mul(in_mat[i * n + j], temp);
			out_mat[i * n + j] = gf_mul(out_mat[i * n + j], temp);
		}

		for (j = 0; j < n; j++) {
			if (j == i)
				continue;

			temp = in_mat[j * n + i];
			for (k = 0; k < n; k++) {
				out_mat[j * n + k] ^= gf_mul(temp, out_mat[i * n + k]);
				in_mat[j * n + k] ^= gf_mul(temp, in_mat[i * n + k]);
			}
		}
	}
	return 0;
}

int** searchData(int* erasedIndexes, int n, int k, int l, int errorLen){
	int i, j, e, p, max;
	int v, u;
	int r = n - k;
	int s = r;
	int m = n / s;
	int** dataIndexes = (int**)malloc(n*sizeof(int*));
	for (i=0; i < n; i++)
		dataIndexes[i] = (int*)malloc(l*sizeof(int));
	int flag = 1;
	if (errorLen > 1) {
		// multiple error nodes
		max = 0;
		for (i=0; i < n; i++) {
			if (max < k) {
				// determine if it is a lost disk
				flag = 1;
				for (j=0; j < errorLen; j++)
					if ( i == erasedIndexes[j])
						flag = 0;
				// get msr data index
				if (flag != 0) {
					for (j=0; j < l; j++)
						dataIndexes[i][j] = 1;
					max++;
				} else {
					for (j=0; j < l; j++)
						dataIndexes[i][j] = 0;
				}
			} else {
				for (j=0; j < l; j++)
					dataIndexes[i][j] = 0;
			}
		}
	} else {
		// single node recovery
		for (i=0; i < n; i++)
			for (j=0; j < l; j++)
				dataIndexes[i][j] = 0;
		u = erasedIndexes[0] % s;
		v = (erasedIndexes[0]-u)/s + 1;
		int Ary[m];
		int temp;
		for (i=0; i < l; i++) {
			e = 0;
			temp = i;
			for (j=0; j < m; j++) Ary[j] = 0;
			while (temp/s != 0) {
				Ary[e] = temp % s;
				temp = temp / s;
				e++;
			}
			Ary[e] = temp % s;
			// compare a_v and u
			if (Ary[v-1] == u) {
				for (p=0; p < n; p++)
					if (p != erasedIndexes[0]) dataIndexes[p][i] = 1;
			}
		}
	}
	return dataIndexes;
}

// Calculates const table gftbl in GF(2^8) from single input A
// gftbl(A) = {A{00}, A{01}, A{02}, ... , A{0f} }, {A{00}, A{10}, A{20}, ... , A{f0} }

void gf_vect_mul_init(unsigned char c, unsigned char *tbl)
{
	unsigned char c2 = (c << 1) ^ ((c & 0x80) ? 0x1d : 0);	//Mult by GF{2}
	unsigned char c4 = (c2 << 1) ^ ((c2 & 0x80) ? 0x1d : 0);	//Mult by GF{2}
	unsigned char c8 = (c4 << 1) ^ ((c4 & 0x80) ? 0x1d : 0);	//Mult by GF{2}

#if __WORDSIZE == 64 || _WIN64 || __x86_64__
	unsigned long long v1, v2, v4, v8, *t;
	unsigned long long v10, v20, v40, v80;
	unsigned char c17, c18, c20, c24;

	t = (unsigned long long *)tbl;

	v1 = c * 0x0100010001000100ull;
	v2 = c2 * 0x0101000001010000ull;
	v4 = c4 * 0x0101010100000000ull;
	v8 = c8 * 0x0101010101010101ull;

	v4 = v1 ^ v2 ^ v4;
	t[0] = v4;
	t[1] = v8 ^ v4;

	c17 = (c8 << 1) ^ ((c8 & 0x80) ? 0x1d : 0);	//Mult by GF{2}
	c18 = (c17 << 1) ^ ((c17 & 0x80) ? 0x1d : 0);	//Mult by GF{2}
	c20 = (c18 << 1) ^ ((c18 & 0x80) ? 0x1d : 0);	//Mult by GF{2}
	c24 = (c20 << 1) ^ ((c20 & 0x80) ? 0x1d : 0);	//Mult by GF{2}

	v10 = c17 * 0x0100010001000100ull;
	v20 = c18 * 0x0101000001010000ull;
	v40 = c20 * 0x0101010100000000ull;
	v80 = c24 * 0x0101010101010101ull;

	v40 = v10 ^ v20 ^ v40;
	t[2] = v40;
	t[3] = v80 ^ v40;

#else // 32-bit or other
	unsigned char c3, c5, c6, c7, c9, c10, c11, c12, c13, c14, c15;
	unsigned char c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30,
	    c31;

	c3 = c2 ^ c;
	c5 = c4 ^ c;
	c6 = c4 ^ c2;
	c7 = c4 ^ c3;

	c9 = c8 ^ c;
	c10 = c8 ^ c2;
	c11 = c8 ^ c3;
	c12 = c8 ^ c4;
	c13 = c8 ^ c5;
	c14 = c8 ^ c6;
	c15 = c8 ^ c7;

	tbl[0] = 0;
	tbl[1] = c;
	tbl[2] = c2;
	tbl[3] = c3;
	tbl[4] = c4;
	tbl[5] = c5;
	tbl[6] = c6;
	tbl[7] = c7;
	tbl[8] = c8;
	tbl[9] = c9;
	tbl[10] = c10;
	tbl[11] = c11;
	tbl[12] = c12;
	tbl[13] = c13;
	tbl[14] = c14;
	tbl[15] = c15;

	c17 = (c8 << 1) ^ ((c8 & 0x80) ? 0x1d : 0);	//Mult by GF{2}
	c18 = (c17 << 1) ^ ((c17 & 0x80) ? 0x1d : 0);	//Mult by GF{2}
	c19 = c18 ^ c17;
	c20 = (c18 << 1) ^ ((c18 & 0x80) ? 0x1d : 0);	//Mult by GF{2}
	c21 = c20 ^ c17;
	c22 = c20 ^ c18;
	c23 = c20 ^ c19;
	c24 = (c20 << 1) ^ ((c20 & 0x80) ? 0x1d : 0);	//Mult by GF{2}
	c25 = c24 ^ c17;
	c26 = c24 ^ c18;
	c27 = c24 ^ c19;
	c28 = c24 ^ c20;
	c29 = c24 ^ c21;
	c30 = c24 ^ c22;
	c31 = c24 ^ c23;

	tbl[16] = 0;
	tbl[17] = c17;
	tbl[18] = c18;
	tbl[19] = c19;
	tbl[20] = c20;
	tbl[21] = c21;
	tbl[22] = c22;
	tbl[23] = c23;
	tbl[24] = c24;
	tbl[25] = c25;
	tbl[26] = c26;
	tbl[27] = c27;
	tbl[28] = c28;
	tbl[29] = c29;
	tbl[30] = c30;
	tbl[31] = c31;

#endif //__WORDSIZE == 64 || _WIN64 || __x86_64__
}

void gf_vect_dot_prod_base(int len, int vlen, unsigned char *v,
			   unsigned char **src, unsigned char *dest)
{
	int i, j;
	unsigned char s;
	for (i = 0; i < len; i++) {
		s = 0;
		for (j = 0; j < vlen; j++)
			s ^= gf_mul(src[j][i], v[j * 32 + 1]);

		dest[i] = s;
	}
}

void gf_vect_mad_base(int len, int vec, int vec_i,
		      unsigned char *v, unsigned char *src, unsigned char *dest)
{
	int i;
	unsigned char s;
	for (i = 0; i < len; i++) {
		s = dest[i];
		s ^= gf_mul(src[i], v[vec_i * 32 + 1]);
		dest[i] = s;
	}
}

void ec_encode_data_base(int len, int srcs, int dests, unsigned char *v,
			 unsigned char **src, unsigned char **dest)
{
	int i, j, l;
	unsigned char s;

	for (l = 0; l < dests; l++) {
		for (i = 0; i < len; i++) {
			s = 0;
			for (j = 0; j < srcs; j++)
				s ^= gf_mul(src[j][i], v[j * 32 + l * srcs * 32 + 1]);

			dest[l][i] = s;
		}
	}
}

void ec_encode_data_update_base(int len, int k, int rows, int vec_i, unsigned char *v,
				unsigned char *data, unsigned char **dest)
{
	int i, l;
	unsigned char s;

	for (l = 0; l < rows; l++) {
		for (i = 0; i < len; i++) {
			s = dest[l][i];
			s ^= gf_mul(data[i], v[vec_i * 32 + l * k * 32 + 1]);

			dest[l][i] = s;
		}
	}
}

void gf_vect_mul_base(int len, unsigned char *a, unsigned char *src, unsigned char *dest)
{
	//2nd element of table array is ref value used to fill it in
	unsigned char c = a[1];
	while (len-- > 0)
		*dest++ = gf_mul(c, *src++);
}

struct slver {
	unsigned short snum;
	unsigned char ver;
	unsigned char core;
};

// Version info
struct slver gf_vect_mul_init_slver_00020035;
struct slver gf_vect_mul_init_slver = { 0x0035, 0x02, 0x00 };

struct slver ec_encode_data_base_slver_00010135;
struct slver ec_encode_data_base_slver = { 0x0135, 0x01, 0x00 };

struct slver gf_vect_mul_base_slver_00010136;
struct slver gf_vect_mul_base_slver = { 0x0136, 0x01, 0x00 };

struct slver gf_vect_dot_prod_base_slver_00010137;
struct slver gf_vect_dot_prod_base_slver = { 0x0137, 0x01, 0x00 };

struct slver gf_mul_slver_00000214;
struct slver gf_mul_slver = { 0x0214, 0x00, 0x00 };

struct slver gf_invert_matrix_slver_00000215;
struct slver gf_invert_matrix_slver = { 0x0215, 0x00, 0x00 };

struct slver gf_gen_rs_matrix_slver_00000216;
struct slver gf_gen_rs_matrix_slver = { 0x0216, 0x00, 0x00 };

struct slver gf_gen_cauchy1_matrix_slver_00000217;
struct slver gf_gen_cauchy1_matrix_slver = { 0x0217, 0x00, 0x00 };
