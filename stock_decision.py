def decision(df, stock_code):
    temp = df
    if temp['ma5'][len(temp) - 1] > temp['ma10'][len(temp) - 1] and temp['ma5'][len(temp) - 2] < temp['ma10'][
        len(temp) - 2]:
        temp['decision'][len(temp) - 1] = 'buy'
    else:
        temp['decision'][len(temp) - 1] = temp['decision'][len(temp) - 2]

    df['decision'][(df.seq == (len(temp) - 1)) & (df.stock_code == stock_code)] = temp['decision'][len(temp) - 1]
    return
