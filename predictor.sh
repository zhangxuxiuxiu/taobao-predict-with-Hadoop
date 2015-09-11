# assuming that current location is the root of project GoodsRecommender

#execution parameter configuration
jar_file="./target/GoodsRecommender-0.1.jar"
main_class="cn.edu.pku.zx.ali.predict.ShoppingGoodsPrediction"
in_dir="./target/input"
out_dir="./target/output"

# phrase one: build project
mvn package

# phrase two: pre-execution environment verification

# if output directory does exist, remove it
if [ -d "$out_dir" ]; then
	rm -r "$out_dir"
fi

# phrase three: execution
hadoop jar "$jar_file" "$main_class" -fs file:/// -jt local "$in_dir" "$out_dir"

cd "target/classes"
java cn.edu.pku.zx.ali.predict.evaluate.FValueEvaluator
cd "../../"
###hadoop jar target/GoodsRecommender-0.1.jar cn.edu.pku.zx.ali.predict.evaluate.ResultSetBuilder -fs file:/// -jt local target/input target/resultset
