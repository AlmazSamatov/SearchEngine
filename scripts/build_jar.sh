if [ "$1" != "" ]; then
    echo "***Start building***"
    rm -dr objects
    mkdir objects
    for i_file in $1; do
        file="${i_file%.*}"
	rm $file.jar
        javac -d objects -cp $(hadoop classpath) $1 &&
        jar -cvf $file.jar objects/$file*.class
	echo "***Done***"
    done
else
    echo "No classes specified"
fi
