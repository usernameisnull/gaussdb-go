.PHONY: create_opengauss
create_opengauss:
	bash ci/setup_test.sh

.PHONY: run_test
run_test:
	bash ci/run_test.sh

all: create_opengauss run_test