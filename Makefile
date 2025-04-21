.PHONY: create_database_and_user
create_database_and_user:
	bash ci/create_database_and_user.sh

.PHONY: run_test
run_test:
	bash ci/run_test.sh