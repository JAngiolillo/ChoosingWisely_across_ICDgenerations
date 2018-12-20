"""
   This block of code builds the fundamental tables that will be examined for CW evaluation.

   It's unclear to me if this code (as currently designed) has to be executed w/in Squirrel, or happens here.
"""
def cwsql_codeviewer():
    print(" As of July 19, 2016\n\
*** Required SQL Commands ***\n\
_____________________________\n\
\n\
\n\
 **First drop old SQL tables ** \n\
DROP TABLE J_cw_rx;\n\
DROP TABLE J_cw_icd; \n\
DROP TABLE J_cw_cpt;\n\
DROP TABLE J_cw_dexa;\n\
DROP TABLE J_cw_adms;\n\
\n\
\n\
 *** Commands to create the tables *** \n\
\n\
\n\
CREATE TABLE J_cw_rx AS (SELECT V_RXSTAR.MRN \n\ \
						,V_RXSTAR.MED_NAME \n\
						,V_RXSTAR.DOSE_FORM \n\
						,V_RXSTAR.RX_DOSE \n\
						,V_RXSTAR.RX_UNIT \n\
						,V_RXSTAR.DISP_AMT \n\
						,V_RXSTAR.DISP_UNIT \n\
						,V_RXSTAR.FREQUENCY \n\
						,V_RXSTAR.DURATION \n\
						,V_RXSTAR.ENTRY_DATE \n\
						,V_RXSTAR.NZ_INSERT_DT \n\
						,V_PATIENT_MASTER.DOB \n\
						,V_PATIENT_MASTER.GENDER \n\
						,EXTRACT(DAY FROM V_RXSTAR.ENTRY_DATE-V_PATIENT_MASTER.DOB)::int as AGEATCLAIM \n\
						FROM V_RXSTAR  \n\
					    INNER JOIN V_PATIENT_MASTER ON V_RXSTAR.MRN = V_PATIENT_MASTER.MRN where V_RXSTAR.ENTRY_DATE > '2015-09-30'); \n\
\n\
CREATE TABLE J_cw_icd AS (SELECT V_ICD10_CODE.MRN \n\
						,V_ICD10_CODE.ENC_ID \n\
						,V_ICD10_CODE.EVT_DATE \n\
						,V_ICD10_CODE.CODE \n\
						,V_ICD10_TO_9_REIMB_VUMC_XREF.ICD9_CODE \n\
                        ,V_PATIENT_MASTER.GENDER \n\
						,V_PATIENT_MASTER.DOB \n\
						,EXTRACT(DAY FROM V_ICD10_CODE.EVT_DATE-V_PATIENT_MASTER.DOB)::int as AGEATCLAIM  \n\
						FROM V_ICD10_CODE \n\
                        INNER JOIN V_ICD10_TO_9_REIMB_VUMC_XREF  \n\
						ON V_ICD10_CODE.CODE = V_ICD10_TO_9_REIMB_VUMC_XREF.ICD10_CODE  \n\
						INNER JOIN V_PATIENT_MASTER ON \n\
                        V_ICD10_CODE.MRN = V_PATIENT_MASTER.MRN);\n\
\n\
CREATE TABLE J_ICDCROSSWALK_FOR_OLDER AS (select * from V_ICD10_TO_9_REIMB_VUMC_XREF where MAPPING_EXPIRATION_DATE is null; \n\
\n\
CREATE TABLE J_cw_icd9_older AS (SELECT V_ICD_CODE.MRN \n\
						,V_ICD_CODE.ENC_ID \n\
						,V_ICD_CODE.EVT_DATE \n\
						,J_ICDCROSSWALK_FOR_OLDER.ICD10_CODE as CODE \n\
						,V_ICD_CODE.CODE as ICD9_CODE \n\
                        		,V_PATIENT_MASTER.GENDER \n\
						,V_PATIENT_MASTER.DOB \n\
						,EXTRACT(DAY FROM V_ICD_CODE.EVT_DATE-V_PATIENT_MASTER.DOB)::int as AGEATCLAIM \n\
						FROM V_ICD_CODE \n\
                        		INNER JOIN J_ICDCROSSWALK_FOR_OLDER \n\
						ON V_ICD_CODE.CODE = J_ICDCROSSWALK_FOR_OLDER.ICD9_CODE \n\
						INNER JOIN V_PATIENT_MASTER ON \n\
                        		V_ICD_CODE.MRN = V_PATIENT_MASTER.MRN \n\
                        		WHERE EVT_DATE < '2015-04-02' AND EVT_DATE > '2014-12-31'); \n\
\n\
CREATE TABLE J_CW_ICD_UNIFIED as (SELECT * from J_CW_ICD WHERE AGEATCLAIM > 5475) \n\
\n\
INSERT INTO J_CW_ICD_UNIFIED \n\
	SELECT J_CW_ICD9_OLDER.MRN \n\
		,J_CW_ICD9_OLDER.ENC_ID \n\
		,J_CW_ICD9_OLDER.EVT_DATE \n\
		,J_CW_ICD9_OLDER.CODE \n\
		,J_CW_ICD9_OLDER.ICD9_CODE \n\
		,J_CW_ICD9_OLDER.GENDER \n\
		,J_CW_ICD9_OLDER.DOB \n\
		,J_CW_ICD9_OLDER.AGEATCLAIM \n\
		FROM J_CW_ICD9_OLDER \n\
		WHERE AGEATCLAIM > 5475; \n\
\n\
CREATE TABLE J_cw_cpt AS (SELECT V_CPT_CODE.MRN \n\
						,V_CPT_CODE.ENC_ID \n\
						,V_CPT_CODE.CODE \n\
						,V_CPT_CODE.EVT_DATE \n\
						,V_PATIENT_MASTER.GENDER \n\
						,V_PATIENT_MASTER.DOB \n\
                        ,EXTRACT(DAY FROM V_CPT_CODE.EVT_DATE-V_PATIENT_MASTER.DOB)::int as AGEATCLAIM \n\
						FROM V_CPT_CODE  \n\
						INNER JOIN V_PATIENT_MASTER ON \n\
                        V_CPT_CODE.MRN = V_PATIENT_MASTER.MRN); \n\
\n\
CREATE TABLE J_cw_dexa AS (SELECT * from V_CPT_CODE WHERE CODE IN ('0028T','76075','76076','77080','77081'));\n\
\n\
CREATE TABLE J_CW_ADMS AS (SELECT * \n\
						FROM V_ENC_MP\n\
						WHERE (PT_CLASS not in ('O','B','E','P','N','U'))\n\
						 OR\n\
   					    (PT_CLASS = 'O' and SRV_CODE IN ('GMD', 'HEM', 'PGS', 'TXN')\n\
   					      and SRV_CODE not in ('OUT', 'ANS','OTO','PAT','ONC', 'VAS', 'PNP', 'EMR','NEP', 'PUL','OBS', 'TXK')));\n\
         ")

