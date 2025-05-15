package agh.wggios.analizadanych.datawriter

class DataWriter {
    def write_to_csv(df: DataFrame. path: String): Unit = {
    df.write.mode("overwrite").csv(path)
    }

}
