X10C=${X10_HOME}/bin/x10c++

FLAGS=-VERBOSE_CHECKS=TRUE -O -NO_CHECKS -noassert -cxx-prearg -O2

SRCS=WordCount.x10 PrimeFactors.x10

EXES=$(SRCS:.x10=)

all: $(EXES)

.SUFFIXES:
.SUFFIXES: .x10

.x10:
	$(X10C) $(FLAGS) -o $@ $@.x10


.PHONY: clean
clean:
	rm -rf *.class *.java $(EXES) *.h *.cc 
