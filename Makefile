sht:
	@echo " Compile ht_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/sht_main.c ./src/hash_file.c ./src/sht_file.c ./src/common.c ./src/blockFunctions.c -lbf -o ./build/runner -O2

ht: clean
	@echo " Compile ht_main ...";
	gcc -g -Werror  -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/ht_main.c ./src/hash_file.c ./src/common.c ./src/blockFunctions.c -lbf -o ./build/runner -O2

bf:
	@echo " Compile bf_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/bf_main.c -lbf -o ./build/runner -O2

myMain: clean
	@echo " Compile myMain ...";
	gcc -g -Werror  -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/my_main.c ./src/hash_file.c ./src/common.c ./src/blockFunctions.c -lbf -o ./build/runner -O2

clean:
	rm -rf build/runner data.db data1.db

run: ht
	./build/runner

valgrind: ht clean
	valgrind  --leak-check=full --track-origins=yes ./build/runner
