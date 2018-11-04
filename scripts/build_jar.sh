if [ "$1" != "" ]; then
    echo "***Start building***"
    for i_file in $1; do
        file="${i_file%.*}"
        res_dir=$file-objects
        rm -dr $res_dir
        mkdir $res_dir
        cp $1 $res_dir/
        cd $res_dir
        javac -cp $(hadoop classpath) $1 &&
        jar -cvf $file.jar $file*.class &&
	echo "***Done***"
    done
else
    echo "No classes specified"
fi
