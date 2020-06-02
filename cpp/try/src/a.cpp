#include<vector>
#include "a.h"
#include<iostream>

int f(int x) {
	return x + 1;
}

void p(std::vector<int> & v) {
	for (auto & i : v)
		std::cout << i << std::endl;
}
