package com.SparkTraining;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 *
 * @author k-15ice
 */
public class Main {

    /**
     * 全都道府県の郵便番号情報CSVより、
     * 2行目のデータ(（旧）郵便番号)が一致するレコード数一覧を取得する。
     * 【前準備】
     * 下記のURLより、全都道府県の郵便番号情報CSVを取得し、
     * プロジェクトルートに格納する。
     * http://www.post.japanpost.jp/zipcode/dl/oogaki/zip/ken_all.zip
     * @param args 
     */
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("parkTraining")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        
        // 全都道府県の郵便番号情報CSVを取得する。
        JavaRDD<String> logData = sc.textFile("KEN_ALL.CSV").cache();
        
        // 旧郵便番号の一覧を取得する。
        JavaRDD<String> PostalCodes = logData.map(x -> x.split(",")[1]);
        
        // 旧郵便番号が一致するレコード数を取得する。
        JavaPairRDD<String, Integer> counts = PostalCodes.mapToPair(
                x -> new Tuple2(x, 1)).reduceByKey((x, y) -> (int)x + (int)y);
        
        // プロジェクトルートに"output"フォルダを作成し、結果ファイルを出力する。
        // ※実行時に"output"フォルダが残っているとエラーになるため毎回削除してください。
        counts.saveAsTextFile("output");
    }

}
