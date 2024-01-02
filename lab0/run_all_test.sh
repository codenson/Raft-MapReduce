for target_test in TestSimple TestDeclarationOfIndependence Test1 Test2 Test3 Test4
do
	echo "----------"$target_test
	go test --run $target_test

done
