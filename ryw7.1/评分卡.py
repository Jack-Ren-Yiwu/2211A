import pandas as pd
# 导入数据
df=pd.read_csv("D:\PythonProject\zhuangao5\cs-training.csv")
print(df)
print(df.columns)
print(df.describe())
print(df.isnull().sum())
states={'ID':'ID',
        'SeriousDlqin2yrs':'好坏客户',
        'RevolvingUtilizationOfUnsecuredLines':'可用额度比值',
        'age':'年龄',
        'NumberOfTime30-59DaysPastDueNotWorse':'逾期30-59天笔数',
        'DebtRatio':'负债率',
        'MonthlyIncome':'月收入',
        'NumberOfOpenCreditLinesAndLoans':'信贷数量',
        'NumberOfTimes90DaysLate':'逾期90天笔数',
        'NumberRealEstateLoansOrLines':'固定资产贷款量',
        'NumberOfTime60-89DaysPastDueNotWorse':'逾期60-89天笔数',
        'NumberOfDependents':'家属数量'}
df.rename(columns=states,inplace=True)
df.head()    #修改英文字段名为中文字段名
print(df.columns)
# 数据预处理
#空值处理
for col in df.columns:
    if df[col].isnull().sum()>0:
        df[col]=df[col].fillna(df[col].mean())
print(df.isnull().sum())
#异常值处理
# for col1 in df.columns:
#     Q1=df[col1].quantile(0.25)
#     Q3=df[col1].quantile(0.75)
#     IQR=Q3-Q1
#     low=Q1-1.5*IQR
#     up=Q3+1.5*IQR
#     out=df[(df[col1]<low)|(df[col1]>up)]
#     df[col1]=df[col1].clip(lower=low,upper=up)
# 探索分析
#将年龄均分成5组，求出每组的总的用户数
age_cut=pd.cut(df['年龄'],5)
age_cut_group=df.groupby(age_cut)["好坏客户"].count()
print(age_cut_group)

age_cut_grouped1=df["好坏客户"].groupby(age_cut).sum()
print(age_cut_grouped1)

df2=pd.merge(pd.DataFrame(age_cut_group),pd.DataFrame(age_cut_grouped1),left_index=True,right_index=True)
df2.rename(columns={'好坏客户_x':'总客户数','好坏客户_y':'坏客户数'},inplace=True)
print(df2)

df2.insert(2,"好客户数",df2["总客户数"]-df2["坏客户数"])
print(df2)
import matplotlib.pyplot as plt
import seaborn as sns
plt.rcParams['font.sans-serif']=['SimHei'] # 用 来 正 常 显 示 中 文 标 签
plt.rcParams['axes.unicode_minus']=False # 用来正常显示负号
corr = df.corr()#计算各变量的相关性系数
xticks = list(corr.index)#x轴标签
yticks = list(corr.index)#y轴标签
fig = plt.figure(figsize=(15,10))
ax1 = fig.add_subplot(1, 1, 1)
sns.heatmap(corr, annot=True, cmap="rainbow",ax=ax1,linewidths=.5, annot_kws={'size': 9, 'weight': 'bold', 'color': 'blue'})
ax1.set_xticklabels(xticks, rotation=35, fontsize=15)
ax1.set_yticklabels(yticks, rotation=0, fontsize=15)
plt.show()

cut1=pd.qcut(df["可用额度比值"],4,labels=False)
cut2=pd.qcut(df["年龄"],8,labels=False)
bins3=[-1,0,1,3,5,13]
cut3=pd.cut(df["逾期30-59天笔数"],bins3,labels=False)
cut4=pd.qcut(df["负债率"],3,labels=False)
cut5=pd.qcut(df["月收入"],4,labels=False)
cut6=pd.qcut(df["信贷数量"],4,labels=False)
bins7=[-1, 0, 1, 3,5, 20]
cut7=pd.cut(df["逾期90天笔数"],bins7,labels=False)
bins8=[-1, 0,1,2, 3, 33]
cut8=pd.cut(df["固定资产贷款量"],bins8,labels=False)
bins9=[-1, 0, 1, 3, 12]
cut9=pd.cut(df["逾期60-89天笔数"],bins9,labels=False)
bins10=[-1, 0, 1, 2, 3, 5, 21]
cut10=pd.cut(df["家属数量"],bins10,labels=False)

import numpy as np
rate=df["好坏客户"].sum()/(df["好坏客户"].count()-df["好坏客户"].sum())
def get_woe_data(cut):
    grouped = pd.crosstab(cut, df["好坏客户"])
    if 0 not in grouped.columns: grouped[0] = 0
    if 1 not in grouped.columns: grouped[1] = 0
    grouped += 0.5
    woe = np.log((grouped[1] / grouped[1].sum()) / (grouped[0] / grouped[0].sum()))
    return woe

cut1_woe=get_woe_data(cut1)
cut2_woe=get_woe_data(cut2)
cut3_woe=get_woe_data(cut3)
cut4_woe=get_woe_data(cut4)
cut5_woe=get_woe_data(cut5)
cut6_woe=get_woe_data(cut6)
cut7_woe=get_woe_data(cut7)
cut8_woe=get_woe_data(cut8)
cut9_woe=get_woe_data(cut9)
cut10_woe=get_woe_data(cut10)
cut1_woe.plot.bar(color='b',alpha=0.3,rot=0)
plt.show()
cut2_woe.plot.bar(color='b',alpha=0.3,rot=0)
plt.show()
cut3_woe.plot.bar(color='b',alpha=0.3,rot=0)
plt.show()

def get_IV_data(cut,cut_woe):
    grouped=df["好坏客户"].groupby(cut,as_index = True).value_counts()
    cut_IV=((grouped.unstack().iloc[:,1]/df["好坏客户"].sum()-grouped.unstack().iloc[:,0]/(df["好坏客户"].count()-df["好坏客户"].sum()))*cut_woe).sum()
    return cut_IV
#计算各分组的IV值
cut1_IV=get_IV_data(cut1,cut1_woe)
cut2_IV=get_IV_data(cut2,cut2_woe)
cut3_IV=get_IV_data(cut3,cut3_woe)
cut4_IV=get_IV_data(cut4,cut4_woe)
cut5_IV=get_IV_data(cut5,cut5_woe)
cut6_IV=get_IV_data(cut6,cut6_woe)
cut7_IV=get_IV_data(cut7,cut7_woe)
cut8_IV=get_IV_data(cut8,cut8_woe)
cut9_IV=get_IV_data(cut9,cut9_woe)
cut10_IV=get_IV_data(cut10,cut10_woe)
IV=pd.DataFrame([cut1_IV,cut2_IV,cut3_IV,cut4_IV,cut5_IV,cut6_IV,cut7_IV,cut8_IV,cut9_IV,cut10_IV],index=['可用额度比值','年龄','逾期30-59天笔数','负债率','月收入','信贷数量','逾期90天笔数','固定资产贷款量','逾期60-89天笔数','家属数量'],columns=['IV'])
iv=IV.plot.bar(color='b',alpha=0.3,rot=30,figsize=(10,5),fontsize=(10))
iv.set_title('特征变量与IV值分布图',fontsize=(15))
iv.set_xlabel('特征变量',fontsize=(15))
iv.set_ylabel('IV',fontsize=(15))
plt.show()
print(IV)
# 特征选择
df_new=pd.DataFrame()   #新建df_new存放woe转换后的数据
def replace_data(cut, cut_woe):
    # cut: 分箱后的 Series（0,1,2...）
    # cut_woe: Series，index=箱号，value=WOE值
    return cut.map(cut_woe)  # 安全 + 高效

df_new["好坏客户"]=df["好坏客户"]
df_new["可用额度比值"]=replace_data(cut1,cut1_woe)
df_new["年龄"]=replace_data(cut2,cut2_woe)
df_new["逾期30-59天笔数"]=replace_data(cut3,cut3_woe)
df_new["负债率"]=replace_data(cut4,cut4_woe)
df_new["月收入"]=replace_data(cut5,cut5_woe)
df_new["信贷数量"]=replace_data(cut6,cut6_woe)
df_new["逾期90天笔数"]=replace_data(cut7,cut7_woe)
df_new["固定资产贷款量"]=replace_data(cut8,cut8_woe)
df_new["逾期60-89天笔数"]=replace_data(cut9,cut9_woe)
df_new["家属数量"]=replace_data(cut10,cut10_woe)
df_new.head()

from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split

x=df_new.iloc[:,1:]
y=df_new.iloc[:,0]
x_train,x_test,y_train,y_test=train_test_split(x,y,test_size=0.2,random_state=42,stratify=y)
model=LogisticRegression()
from sklearn.impute import SimpleImputer

# 用中位数填充（适用于大多数数值特征）
imputer = SimpleImputer(strategy='median')
x_train = imputer.fit_transform(x_train)
x_test = imputer.transform(x_test)
clf=model.fit(x_train,y_train)
print('测试成绩：{}'.format(clf.score(x_test,y_test)))
# 模型训练
coe=clf.coef_
print(coe)
y_pred=clf.predict(x_test)
# 模型评估
from sklearn.metrics import roc_curve, auc
fpr, tpr, threshold = roc_curve(y_test, y_pred)
roc_auc = auc(fpr, tpr)
plt.plot(fpr, tpr, color='darkorange',label='ROC curve (area = %0.2f)' % roc_auc)
plt.plot([0, 1], [0, 1], color='navy',  linestyle='--')
plt.xlim([0.0, 1.0])
plt.ylim([0.0, 1.0])
plt.xlabel('False Positive Rate')
plt.ylabel('True Positive Rate')
plt.title('ROC_curve')
plt.legend(loc="lower right")
plt.show()
print(roc_auc)
fig, ax = plt.subplots()
ax.plot(1 - threshold, tpr, label='tpr') # ks曲线要按照预测概率降序排列，所以需要1-threshold镜像
ax.plot(1 - threshold, fpr, label='fpr')
ax.plot(1 - threshold, tpr-fpr,label='KS')
plt.xlabel('score')
plt.title('KS Curve')
plt.ylim([0.0, 1.0])
plt.figure(figsize=(20,20))
legend = ax.legend(loc='upper left')
plt.show()
print(max(tpr-fpr))
# 模型结果转评分
factor = 20 / np.log(2)
offset = 600 - 20 * np.log(20) / np.log(2)
def get_score(coe,woe,factor):
    scores=[]
    for w in woe:
        score=round(coe*w*factor,0)
        scores.append(score)
    return scores
x1 = get_score(coe[0][0], cut1_woe, factor)
x2 = get_score(coe[0][1], cut2_woe, factor)
x3 = get_score(coe[0][2], cut3_woe, factor)
x4 = get_score(coe[0][3], cut4_woe, factor)
x5 = get_score(coe[0][4], cut5_woe, factor)
x6 = get_score(coe[0][5], cut6_woe, factor)
x7 = get_score(coe[0][6], cut7_woe, factor)
x8 = get_score(coe[0][7], cut8_woe, factor)
x9 = get_score(coe[0][8], cut9_woe, factor)
x10 = get_score(coe[0][9], cut10_woe, factor)
print("可用额度比值对应的分数:{}".format(x1))
print("年龄对应的分数:{}".format(x2))
print("逾期30-59天笔数对应的分数:{}".format(x3))
print("负债率对应的分数:{}".format(x4))
print("月收入对应的分数:{}".format(x5))
print("信贷数量对应的分数:{}".format(x6))
print("逾期90天笔数对应的分数:{}".format(x7))
print("固定资产贷款量对应的分数:{}".format(x8))
print("逾期60-89天笔数对应的分数:{}".format(x9))
print("家属数量对应的分数:{}".format(x10))
# 计算用户总分
def compute_score(series,bins,score):
    list = []
    i = 0
    while i < len(series):
        value = series[i]
        j = len(bins) - 2
        m = len(bins) - 2
        while j >= 0:
            if value >= bins[j]:
                j = -1
            else:
                j -= 1
                m -= 1
        list.append(score[m])
        i += 1
    return list
bins = pd.qcut(df["可用额度比值"], 4, retbins=True)[1]
binned = pd.cut(df["可用额度比值"], bins=bins, labels=False, include_lowest=True)
score_map = dict(zip(range(len(x1)), x1))
df_new['可用额度比值_得分'] = binned.map(score_map)
print(df_new[['可用额度比值', '可用额度比值_得分']].head())
