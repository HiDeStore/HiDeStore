CC := gcc
CFLAGS := -g `pkg-config --cflags --libs glib-2.0`
LDFLAGS := -lpthread -lcrypto -lm


SOURCE := $(wildcard ./src/*.c ./src/chunking/*.c ./src/fsl/*.c ./src/index/*.c ./src/recipe/*.c ./src/storage/*.c ./src/utils/*.c ./src/lpf/*.c)
O_DIR = ./build

all:
	$(CC) $(CFLAGS) $(SOURCE) -o ./build/destor $(LDFLAGS)



.PHONY: clean
clean:
	rm -f $(O_DIR)/*