import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings("ignore")
from scipy import stats

class Data:
    def __init__(self):
        pass
    
    def consistent_tickers(self,ENV,array=[],ar=[]):
        for i in range(int(ENV.fiscalyear.min().strftime('%Y')),int(ENV.fiscalyear.max().strftime('%Y'))+1):array.append(ENV.set_index('fiscalyear')[str(i)].ticker.unique().tolist())
        for i in range(len(array)):array[i]=set(array[i])
        for i in range(len(array)):ar=set.intersection(array[i])
        return list(ar)
    
    def X(self,path='Environmental_Sector_Data.csv'):
        ENV=pd.read_csv(path,parse_dates=['Fiscal Year (fiscalyear)','Period End Date (periodenddate)'])
        #Only US
        ENV=ENV[(ENV.filter(regex='incorporation_country')=="United States").values]
        #Only Currently Operating Companies. 
        ENV=ENV[(ENV.filter(regex='status')).values==(ENV.filter(regex='status')).squeeze().unique()[:2]]
        ENV.rename(columns={'Fiscal Year (fiscalyear)':'fiscalyear','Period End Date (periodenddate)':'periodenddate','Simple Industry (simpleindustry)':'simpleindustry','Ticker (ticker)':'ticker'},inplace=True)
        cticks=self.consistent_tickers(ENV)
        ENV=ENV[ENV.ticker.isin(cticks)]
        return ENV
    
    def Y(self,path='CPIUS.csv'):
        CPI=pd.read_csv(path,parse_dates=['DATE'])
        CPI.DATE=CPI.DATE.dt.to_period('M')
        CPI.set_index('DATE',inplace=True)
        return CPI[str(int(self.X().fiscalyear.min().strftime('%Y'))):str(int(self.X().fiscalyear.max().strftime('%Y')))]
    
    def intensity_df(self):
        return self.X().filter(regex='Intensity')
    
    def absolute_df(self):
        return self.X().filter(regex='Absolute')
    
    def aggregated_df(self,flag='all',aggregation='sum'):
        if flag=='all':
            data=self.X()
        elif flag =='intensity':
            data=self.intensity_df()
        elif flag=='absolute_df':
            data=self.absolute_df()
        
        if aggregation=='sum':
            return data.sort_values('periodenddate').groupby(['fiscalyear','simpleindustry']).sum().reset_index()
        else:
            return data.sort_values('periodenddate').groupby(['fiscalyear','simpleindustry']).mean().reset_index()
    
    def transformation_df(self,outlier_removal=False,z_score_threshold=3):
        if outlier_removal:
            data=self.aggregated_df()
            data = data[(np.abs(stats.zscore(data.select_dtypes(exclude=['object','datetime64[ns]']))) < z_score_threshold).all(axis=1)]
        else:
            data=self.aggregated_df()
            
        og=data
        agg=og.select_dtypes(exclude=['object','datetime64[ns]','string'])
        single_diff=agg.diff().bfill().add_suffix('_diff')
        double_diff=agg.diff().diff().bfill().add_suffix('_doublediff')
        pct_change=agg.pct_change().bfill().add_suffix('_pct_change')
        rawdiff=(agg.diff().bfill()*agg).add_suffix('_rawxdiff')
        rawdiffdiff=(agg.diff().bfill()*agg).add_suffix('_rawxdoublediff')
        aggregation_level1=pd.concat([og,single_diff,double_diff,pct_change,rawdiff,rawdiffdiff],axis=1)
        aggregation_level2=aggregation_level1.select_dtypes(exclude=['object','datetime64[ns]','string']).apply(lambda x:x**2).add_suffix('_squared')
        aggregation_level3=aggregation_level1.select_dtypes(exclude=['object','datetime64[ns]','string']).apply(lambda x:x**3).add_suffix('_cubed')
        aggregated=pd.concat([aggregation_level1,aggregation_level2,aggregation_level3],axis=1)
        return aggregated
          
            
    