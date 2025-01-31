#!/bin/bash
set -e

mkdir -vp rdr-lib/tests/fixtures
pushd rdr-lib/tests/fixtures
curl -fL -O https://www.ssec.wisc.edu/~brucef/testdata/RCRIS-RNSCA_j02_d20240627_t1930197_e1943077_b00001_c20240627194303766000_drlu_ops.h5
popd

