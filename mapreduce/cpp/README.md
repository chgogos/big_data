# C++ examples

## map-filter-reduce example (mr_example.cpp)

[1,2,3,4,5,6] -> transform -> [1,4,9,25,36] -> copy_if -> [4, 9, 25,36] -> accumulate -> 90

    g++ mr_example.cpp -o mr_example -std=c++11
    ./mr_example