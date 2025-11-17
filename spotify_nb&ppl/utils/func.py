class reuse:
    def dropCols(self,df,cols):
        df=df.drop(*cols)
        return df