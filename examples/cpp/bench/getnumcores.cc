#include <iostream>

#include <errno.h>
#include <unistd.h>


int main()
{
    int ncpus = static_cast<int>(sysconf(_SC_NPROCESSORS_ONLN));
    if (ncpus < 1) {
        std::cout << "Cannot determine number of CPUs" << std::endl;
        return -1;
    }

    std::cout << "Num cores: " << ncpus << std::endl;


    return 0;
}