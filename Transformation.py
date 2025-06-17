def transform(result):
    try:
            print("transform lower case to upper case")
            temp=result.copy()
            temp.columns=[col.upper() for col in temp.columns]
            print("Transformation done")
            return temp
    except Exception as e:
        print("error")
        return None
