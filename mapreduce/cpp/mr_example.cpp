#include <iostream>
#include <vector>
#include <algorithm>  // transform, copy_if
#include <numeric> // accumulate

using namespace std;

void print_vector(string msg, vector<int> v){
    cout << msg << ":";
    for(int x: v)
        cout << x << " ";
    cout << endl;
}

int main() {
    vector<int> v1{1,2,3,4,5,6};
    print_vector("v1", v1);
    
    vector<int> v2;
    v2.resize(v1.size());
    transform(v1.begin(), v1.end(), v2.begin(), [](int x){return x*x;});
    print_vector("v2", v2);

    vector<int> v3;
    copy_if(v2.begin(), v2.end(), back_inserter(v3), [](int x){return x > 3;});
    print_vector("v3", v3);

    cout << "The sum is " <<  accumulate(v3.begin(), v3.end(), 0) << endl;

}
