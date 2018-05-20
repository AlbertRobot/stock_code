def decision (df,stocklist_select):
    for i in range(len(stocklist_select)-1):
        stockcode=stocklist_select.iloc[i,0]
        temp=df[df['stock_code']==stockcode]
        if temp['ma5'][len(temp)-1]>temp['ma10'][len(temp)-1] and temp['ma5'][len(temp)-2]<temp['ma10'][len(temp)-2]:
            temp['decision'][len(temp)-1]='buy'
        else:
            temp['decision'][len(temp)-1]= temp['decision'][len(temp)-2]
                
        df['decision'][(df.seq==(len(temp)-1) ) & (df.stock_code==stockcode)]=temp['decision'][len(temp)-1]
    return
    

            
        
