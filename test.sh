#!/bin/bash
#unzip $1.zip
#cd $1
cd codebase
cd rbf
make clean
make
cd ../rm
make clean
make
./rmtest_create_tables
./rmtest_00
./rmtest_01
./rmtest_02
./rmtest_03
./rmtest_04
./rmtest_05
./rmtest_06
./rmtest_07
./rmtest_08a
./rmtest_08b
./rmtest_09
./rmtest_10
./rmtest_11
./rmtest_12
./rmtest_13
./rmtest_14
./rmtest_15
./rmtest_16
./rmtest_extra_1
./rmtest_extra_2
./rmtest_extra_3
