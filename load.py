def load(transform,file_path="transformed_data.csv"):
    if transform is not None:
        try:
            transform.to_csv(file_path,index=False)
            print("we have loaded the transform file")

        except Exception as e:
            print(f"error occurred during loading the file:{e}")



