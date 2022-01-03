sht: clean
	@echo " Compile sht_main ...";
	gcc -g -Werror -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/sht_main.c ./src/hash_file.c ./src/sht_file.c ./src/common.c ./src/blockFunctions.c ./src/sht_blockFunctions.c ./src/shtInsertFunctions.c -lbf -o ./build/runner -O2

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
	rm -rf build/runner data.db data1.db Sec_data.db Sec_data1.db

run: ht
	./build/runner

runsht: sht
	./build/runner

valgrind_sht: sht clean
	valgrind  --leak-check=full --show-leak-kinds=all --track-origins=yes ./build/runner

valgrind_ht: ht clean
	valgrind  --leak-check=full --show-leak-kinds=all --track-origins=yes ./build/runner
