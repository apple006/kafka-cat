all:
	cd src && $(MAKE) kafka-cat 
clean:
	cd src && $(MAKE) $@
