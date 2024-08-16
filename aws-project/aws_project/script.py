from csv_to_parquet import procces_csv

print("o codigo esta sendo executado")
def main():

    csv_file = 'C:\\Users\\lurch\\Documents\\aws-project\\data\\Netflix_movies_and_tv_shows_clustering.csv'
    output_dir = 'C:\\Users\\lurch\\Documents\\aws-project\\output_files'
    bucket_name = 'bucket-lucas-dastre'
    s3_key = 'parquet_files/MovieAndTv.parquet'

    procces_csv(csv_file, output_dir , bucket_name , s3_key) 


if __name__ == "__main__":
    main()
