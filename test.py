<<<<<<< HEAD
import pandas as pd


df = pd.read_xml(r"E:\Análise Backup\george valda\data-133907148038772453\files.xml")

print(df.head())
print(len(df))
=======
from striprtf.striprtf import rtf_to_text

a = r"{\rtf1\ansi\ansicpg1252\uc1\deff0{\fonttbl{\f0\fnil\fcharset0\fprq2 Arial;}{\f1\fswiss\fcharset0\fprq2 Tahoma;}{\f2\froman\fcharset2\fprq2 Symbol;}}{\colortbl;}{\stylesheet{\s0\itap0\nowidctlpar\f0\fs24 [Normal];}{\*\cs10\additive Default Paragraph Font;}}{\*\generator TX_RTF32 19.0.542.501;}\paperw11907\paperh16840\margl567\margt567\margr567\margb567\deftab1134\widowctrl\lytexcttp\formshade\sectd\headery720\footery720\pgwsxn11907\pghsxn16840\marglsxn567\margtsxn567\margrsxn567\margbsxn567\pgbrdropt32\pard\itap0\nowidctlpar\plain\f1\fs20 PCTE SEM QUEIXA\par DUM 05/07   MAC VASECTOMIA\par AP NDN   AF NDN   H/F TABAGISTA\par IG IPN\par MAMAS NDN\par ESP NDN\par CD SOL EXAMES\par }"

a = rtf_to_text(a)
print(a)
>>>>>>> 6fa5be697074c24f52242cb2041180a81e9e6518
