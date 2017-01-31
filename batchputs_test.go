package batchputs_test

import (
	"encoding/csv"
	"os"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"

	"github.com/jinzhu/gorm"
	"github.com/theplant/batchputs"
	"github.com/theplant/testingutils"
)

type Country struct {
	Code         string `gorm:"primary_key" sql:"size:50"`
	ShortName    string `sql:"size:500"`
	SpecialNotes string `sql:"size:2000"`
	Region       string `sql:"size:500"`
	IncomeGroup  string `sql:"size:500"`
	Count        int
	AvgAge       float64
}

var cases = []struct {
	columns []string
	rows    [][]interface{}
	expects string
}{
	{
		columns: []string{"code", "short_name"},
		rows: toArray(`
AFG	Afghanistan
AGO	Angola
ALB	Albania
ARG	Argentina
ARM	Armenia
AZE	Azerbaijan
BEN	Benin
BFA	Burkina Faso
BGD	Bangladesh
BGR	Bulgaria
BHS	Bahamas, The
BIH	Bosnia and Herzegovina
BLR	Belarus
BLZ	Belize
        `),
		expects: `[
	{
		"Code": "AFG",
		"ShortName": "Afghanistan",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "AGO",
		"ShortName": "Angola",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "ALB",
		"ShortName": "Albania",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "ARG",
		"ShortName": "Argentina",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "ARM",
		"ShortName": "Armenia",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "AZE",
		"ShortName": "Azerbaijan",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BEN",
		"ShortName": "Benin",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BFA",
		"ShortName": "Burkina Faso",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BGD",
		"ShortName": "Bangladesh",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BGR",
		"ShortName": "Bulgaria",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BHS",
		"ShortName": "Bahamas, The",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BIH",
		"ShortName": "Bosnia and Herzegovina",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BLR",
		"ShortName": "Belarus",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BLZ",
		"ShortName": "Belize",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	}
]`,
	},
	{
		columns: []string{"code", "special_notes", "region"},
		rows: toArray(`
ARG	Argentina.  Region: Latin America & Caribbean.  Income group: High income: nonOECD.  Lending category: IBRD.  Currency unit: Argentine peso.  National accounts base year: 2004.  National accounts reference year: .  Latest population census: 2010.  Latest household survey: Multiple Indicator Cluster Survey (MICS), 2011/12.  Special notes: The base year has changed to 2004.	Latin America & Caribbean
ARM	Armenia.  Region: Europe & Central Asia.  Income group: Lower middle income.  Lending category: IBRD.  Currency unit: Armenian dram.  National accounts base year: Original chained constant price data are rescaled..  National accounts reference year: 1996.  Latest population census: 2011.  Latest household survey: Demographic and Health Survey (DHS), 2010.	Europe & Central Asia
AZE	Azerbaijan.  Region: Europe & Central Asia.  Income group: Upper middle income.  Lending category: IBRD.  Currency unit: New Azeri manat.  National accounts base year: 2000.  National accounts reference year: .  Latest population census: 2009.  Latest household survey: Demographic and Health Survey (DHS), 2006.  Special notes: April 2012 database update: National accounts historical expenditure series in constant prices were revised in line with State Statistical Committee data that were not previously available.	Europe & Central Asia
BEN	Benin.  Region: Sub-Saharan Africa.  Income group: Low income.  Lending category: IDA.  Currency unit: CFA franc.  National accounts base year: 1985.  National accounts reference year: .  Latest population census: 2013.  Latest household survey: Multiple Indicator Cluster Survey (MICS), 2014.	Sub-Saharan Africa
BFA	Burkina Faso.  Region: Sub-Saharan Africa.  Income group: Low income.  Lending category: IDA.  Currency unit: CFA franc.  National accounts base year: 1999.  National accounts reference year: .  Latest population census: 2006.  Latest household survey: Malaria Indicator Survey (MIS), 2014.	Sub-Saharan Africa
BGD	Bangladesh.  Region: South Asia.  Income group: Lower middle income.  Lending category: IDA.  Currency unit: Bangladeshi taka.  National accounts base year: 2005/06.  National accounts reference year: .  Latest population census: 2011.  Latest household survey: Demographic and Health Survey (DHS), 2014; HIV/Maternal and Child Health (HIV/MCH) Service Provision Assessments (SPA), 2014.  Special notes: Fiscal year end: June 30; reporting period for national accounts data: FY. The new base year is 2005/06.	South Asia
BGR	Bulgaria.  Region: Europe & Central Asia.  Income group: Upper middle income.  Lending category: IBRD.  Currency unit: Bulgarian lev.  National accounts base year: Original chained constant price data are rescaled..  National accounts reference year: 2010.  Latest population census: 2011.  Latest household survey: Living Standards Measurement Study Survey (LSMS), 2007.  Special notes: The new reference year for chain linked series is 2010. April 2011 database update: The National Statistical Office revised national accounts data from 1995 onward. GDP in current prices were about 4 percent higher than previous estimates.	Europe & Central Asia
        `),
		expects: `[
	{
		"Code": "AFG",
		"ShortName": "Afghanistan",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "AGO",
		"ShortName": "Angola",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "ALB",
		"ShortName": "Albania",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "ARG",
		"ShortName": "Argentina",
		"SpecialNotes": "Argentina.  Region: Latin America \u0026 Caribbean.  Income group: High income: nonOECD.  Lending category: IBRD.  Currency unit: Argentine peso.  National accounts base year: 2004.  National accounts reference year: .  Latest population census: 2010.  Latest household survey: Multiple Indicator Cluster Survey (MICS), 2011/12.  Special notes: The base year has changed to 2004.",
		"Region": "Latin America \u0026 Caribbean",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "ARM",
		"ShortName": "Armenia",
		"SpecialNotes": "Armenia.  Region: Europe \u0026 Central Asia.  Income group: Lower middle income.  Lending category: IBRD.  Currency unit: Armenian dram.  National accounts base year: Original chained constant price data are rescaled..  National accounts reference year: 1996.  Latest population census: 2011.  Latest household survey: Demographic and Health Survey (DHS), 2010.",
		"Region": "Europe \u0026 Central Asia",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "AZE",
		"ShortName": "Azerbaijan",
		"SpecialNotes": "Azerbaijan.  Region: Europe \u0026 Central Asia.  Income group: Upper middle income.  Lending category: IBRD.  Currency unit: New Azeri manat.  National accounts base year: 2000.  National accounts reference year: .  Latest population census: 2009.  Latest household survey: Demographic and Health Survey (DHS), 2006.  Special notes: April 2012 database update: National accounts historical expenditure series in constant prices were revised in line with State Statistical Committee data that were not previously available.",
		"Region": "Europe \u0026 Central Asia",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BEN",
		"ShortName": "Benin",
		"SpecialNotes": "Benin.  Region: Sub-Saharan Africa.  Income group: Low income.  Lending category: IDA.  Currency unit: CFA franc.  National accounts base year: 1985.  National accounts reference year: .  Latest population census: 2013.  Latest household survey: Multiple Indicator Cluster Survey (MICS), 2014.",
		"Region": "Sub-Saharan Africa",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BFA",
		"ShortName": "Burkina Faso",
		"SpecialNotes": "Burkina Faso.  Region: Sub-Saharan Africa.  Income group: Low income.  Lending category: IDA.  Currency unit: CFA franc.  National accounts base year: 1999.  National accounts reference year: .  Latest population census: 2006.  Latest household survey: Malaria Indicator Survey (MIS), 2014.",
		"Region": "Sub-Saharan Africa",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BGD",
		"ShortName": "Bangladesh",
		"SpecialNotes": "Bangladesh.  Region: South Asia.  Income group: Lower middle income.  Lending category: IDA.  Currency unit: Bangladeshi taka.  National accounts base year: 2005/06.  National accounts reference year: .  Latest population census: 2011.  Latest household survey: Demographic and Health Survey (DHS), 2014; HIV/Maternal and Child Health (HIV/MCH) Service Provision Assessments (SPA), 2014.  Special notes: Fiscal year end: June 30; reporting period for national accounts data: FY. The new base year is 2005/06.",
		"Region": "South Asia",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BGR",
		"ShortName": "Bulgaria",
		"SpecialNotes": "Bulgaria.  Region: Europe \u0026 Central Asia.  Income group: Upper middle income.  Lending category: IBRD.  Currency unit: Bulgarian lev.  National accounts base year: Original chained constant price data are rescaled..  National accounts reference year: 2010.  Latest population census: 2011.  Latest household survey: Living Standards Measurement Study Survey (LSMS), 2007.  Special notes: The new reference year for chain linked series is 2010. April 2011 database update: The National Statistical Office revised national accounts data from 1995 onward. GDP in current prices were about 4 percent higher than previous estimates.",
		"Region": "Europe \u0026 Central Asia",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BHS",
		"ShortName": "Bahamas, The",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BIH",
		"ShortName": "Bosnia and Herzegovina",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BLR",
		"ShortName": "Belarus",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BLZ",
		"ShortName": "Belize",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	}
]`,
	},
	{
		columns: []string{"code", "short_name"},
		rows: toArray(`
AFG	Afghanistan
AGO	Angola
ALB	Albania
ARG	Argentina
        `),
		expects: `[
	{
		"Code": "AFG",
		"ShortName": "Afghanistan",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "AGO",
		"ShortName": "Angola",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "ALB",
		"ShortName": "Albania",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "ARG",
		"ShortName": "Argentina",
		"SpecialNotes": "Argentina.  Region: Latin America \u0026 Caribbean.  Income group: High income: nonOECD.  Lending category: IBRD.  Currency unit: Argentine peso.  National accounts base year: 2004.  National accounts reference year: .  Latest population census: 2010.  Latest household survey: Multiple Indicator Cluster Survey (MICS), 2011/12.  Special notes: The base year has changed to 2004.",
		"Region": "Latin America \u0026 Caribbean",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "ARM",
		"ShortName": "Armenia",
		"SpecialNotes": "Armenia.  Region: Europe \u0026 Central Asia.  Income group: Lower middle income.  Lending category: IBRD.  Currency unit: Armenian dram.  National accounts base year: Original chained constant price data are rescaled..  National accounts reference year: 1996.  Latest population census: 2011.  Latest household survey: Demographic and Health Survey (DHS), 2010.",
		"Region": "Europe \u0026 Central Asia",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "AZE",
		"ShortName": "Azerbaijan",
		"SpecialNotes": "Azerbaijan.  Region: Europe \u0026 Central Asia.  Income group: Upper middle income.  Lending category: IBRD.  Currency unit: New Azeri manat.  National accounts base year: 2000.  National accounts reference year: .  Latest population census: 2009.  Latest household survey: Demographic and Health Survey (DHS), 2006.  Special notes: April 2012 database update: National accounts historical expenditure series in constant prices were revised in line with State Statistical Committee data that were not previously available.",
		"Region": "Europe \u0026 Central Asia",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BEN",
		"ShortName": "Benin",
		"SpecialNotes": "Benin.  Region: Sub-Saharan Africa.  Income group: Low income.  Lending category: IDA.  Currency unit: CFA franc.  National accounts base year: 1985.  National accounts reference year: .  Latest population census: 2013.  Latest household survey: Multiple Indicator Cluster Survey (MICS), 2014.",
		"Region": "Sub-Saharan Africa",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BFA",
		"ShortName": "Burkina Faso",
		"SpecialNotes": "Burkina Faso.  Region: Sub-Saharan Africa.  Income group: Low income.  Lending category: IDA.  Currency unit: CFA franc.  National accounts base year: 1999.  National accounts reference year: .  Latest population census: 2006.  Latest household survey: Malaria Indicator Survey (MIS), 2014.",
		"Region": "Sub-Saharan Africa",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BGD",
		"ShortName": "Bangladesh",
		"SpecialNotes": "Bangladesh.  Region: South Asia.  Income group: Lower middle income.  Lending category: IDA.  Currency unit: Bangladeshi taka.  National accounts base year: 2005/06.  National accounts reference year: .  Latest population census: 2011.  Latest household survey: Demographic and Health Survey (DHS), 2014; HIV/Maternal and Child Health (HIV/MCH) Service Provision Assessments (SPA), 2014.  Special notes: Fiscal year end: June 30; reporting period for national accounts data: FY. The new base year is 2005/06.",
		"Region": "South Asia",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BGR",
		"ShortName": "Bulgaria",
		"SpecialNotes": "Bulgaria.  Region: Europe \u0026 Central Asia.  Income group: Upper middle income.  Lending category: IBRD.  Currency unit: Bulgarian lev.  National accounts base year: Original chained constant price data are rescaled..  National accounts reference year: 2010.  Latest population census: 2011.  Latest household survey: Living Standards Measurement Study Survey (LSMS), 2007.  Special notes: The new reference year for chain linked series is 2010. April 2011 database update: The National Statistical Office revised national accounts data from 1995 onward. GDP in current prices were about 4 percent higher than previous estimates.",
		"Region": "Europe \u0026 Central Asia",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BHS",
		"ShortName": "Bahamas, The",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BIH",
		"ShortName": "Bosnia and Herzegovina",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BLR",
		"ShortName": "Belarus",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BLZ",
		"ShortName": "Belize",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	}
]`,
	},
	{
		columns: []string{"code", "count", "avg_age"},
		rows: toArray(`
AFG	12	99.01
AGO	13	99.02
ALB	14	99.03
ARG	15	99.04
        `),
		expects: `[
	{
		"Code": "AFG",
		"ShortName": "Afghanistan",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 12,
		"AvgAge": 99.01
	},
	{
		"Code": "AGO",
		"ShortName": "Angola",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 13,
		"AvgAge": 99.02
	},
	{
		"Code": "ALB",
		"ShortName": "Albania",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 14,
		"AvgAge": 99.03
	},
	{
		"Code": "ARG",
		"ShortName": "Argentina",
		"SpecialNotes": "Argentina.  Region: Latin America \u0026 Caribbean.  Income group: High income: nonOECD.  Lending category: IBRD.  Currency unit: Argentine peso.  National accounts base year: 2004.  National accounts reference year: .  Latest population census: 2010.  Latest household survey: Multiple Indicator Cluster Survey (MICS), 2011/12.  Special notes: The base year has changed to 2004.",
		"Region": "Latin America \u0026 Caribbean",
		"IncomeGroup": "",
		"Count": 15,
		"AvgAge": 99.04
	},
	{
		"Code": "ARM",
		"ShortName": "Armenia",
		"SpecialNotes": "Armenia.  Region: Europe \u0026 Central Asia.  Income group: Lower middle income.  Lending category: IBRD.  Currency unit: Armenian dram.  National accounts base year: Original chained constant price data are rescaled..  National accounts reference year: 1996.  Latest population census: 2011.  Latest household survey: Demographic and Health Survey (DHS), 2010.",
		"Region": "Europe \u0026 Central Asia",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "AZE",
		"ShortName": "Azerbaijan",
		"SpecialNotes": "Azerbaijan.  Region: Europe \u0026 Central Asia.  Income group: Upper middle income.  Lending category: IBRD.  Currency unit: New Azeri manat.  National accounts base year: 2000.  National accounts reference year: .  Latest population census: 2009.  Latest household survey: Demographic and Health Survey (DHS), 2006.  Special notes: April 2012 database update: National accounts historical expenditure series in constant prices were revised in line with State Statistical Committee data that were not previously available.",
		"Region": "Europe \u0026 Central Asia",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BEN",
		"ShortName": "Benin",
		"SpecialNotes": "Benin.  Region: Sub-Saharan Africa.  Income group: Low income.  Lending category: IDA.  Currency unit: CFA franc.  National accounts base year: 1985.  National accounts reference year: .  Latest population census: 2013.  Latest household survey: Multiple Indicator Cluster Survey (MICS), 2014.",
		"Region": "Sub-Saharan Africa",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BFA",
		"ShortName": "Burkina Faso",
		"SpecialNotes": "Burkina Faso.  Region: Sub-Saharan Africa.  Income group: Low income.  Lending category: IDA.  Currency unit: CFA franc.  National accounts base year: 1999.  National accounts reference year: .  Latest population census: 2006.  Latest household survey: Malaria Indicator Survey (MIS), 2014.",
		"Region": "Sub-Saharan Africa",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BGD",
		"ShortName": "Bangladesh",
		"SpecialNotes": "Bangladesh.  Region: South Asia.  Income group: Lower middle income.  Lending category: IDA.  Currency unit: Bangladeshi taka.  National accounts base year: 2005/06.  National accounts reference year: .  Latest population census: 2011.  Latest household survey: Demographic and Health Survey (DHS), 2014; HIV/Maternal and Child Health (HIV/MCH) Service Provision Assessments (SPA), 2014.  Special notes: Fiscal year end: June 30; reporting period for national accounts data: FY. The new base year is 2005/06.",
		"Region": "South Asia",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BGR",
		"ShortName": "Bulgaria",
		"SpecialNotes": "Bulgaria.  Region: Europe \u0026 Central Asia.  Income group: Upper middle income.  Lending category: IBRD.  Currency unit: Bulgarian lev.  National accounts base year: Original chained constant price data are rescaled..  National accounts reference year: 2010.  Latest population census: 2011.  Latest household survey: Living Standards Measurement Study Survey (LSMS), 2007.  Special notes: The new reference year for chain linked series is 2010. April 2011 database update: The National Statistical Office revised national accounts data from 1995 onward. GDP in current prices were about 4 percent higher than previous estimates.",
		"Region": "Europe \u0026 Central Asia",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BHS",
		"ShortName": "Bahamas, The",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BIH",
		"ShortName": "Bosnia and Herzegovina",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BLR",
		"ShortName": "Belarus",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BLZ",
		"ShortName": "Belize",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	}
]`,
	},
	{
		columns: []string{"code", "count", "avg_age"},
		rows: toArray(`
AFG	12	99.01
AGO	13	99.02
ALB	14	99.09
        `),
		expects: `[
	{
		"Code": "AFG",
		"ShortName": "Afghanistan",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 12,
		"AvgAge": 99.01
	},
	{
		"Code": "AGO",
		"ShortName": "Angola",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 13,
		"AvgAge": 99.02
	},
	{
		"Code": "ALB",
		"ShortName": "Albania",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 14,
		"AvgAge": 99.09
	},
	{
		"Code": "ARG",
		"ShortName": "Argentina",
		"SpecialNotes": "Argentina.  Region: Latin America \u0026 Caribbean.  Income group: High income: nonOECD.  Lending category: IBRD.  Currency unit: Argentine peso.  National accounts base year: 2004.  National accounts reference year: .  Latest population census: 2010.  Latest household survey: Multiple Indicator Cluster Survey (MICS), 2011/12.  Special notes: The base year has changed to 2004.",
		"Region": "Latin America \u0026 Caribbean",
		"IncomeGroup": "",
		"Count": 15,
		"AvgAge": 99.04
	},
	{
		"Code": "ARM",
		"ShortName": "Armenia",
		"SpecialNotes": "Armenia.  Region: Europe \u0026 Central Asia.  Income group: Lower middle income.  Lending category: IBRD.  Currency unit: Armenian dram.  National accounts base year: Original chained constant price data are rescaled..  National accounts reference year: 1996.  Latest population census: 2011.  Latest household survey: Demographic and Health Survey (DHS), 2010.",
		"Region": "Europe \u0026 Central Asia",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "AZE",
		"ShortName": "Azerbaijan",
		"SpecialNotes": "Azerbaijan.  Region: Europe \u0026 Central Asia.  Income group: Upper middle income.  Lending category: IBRD.  Currency unit: New Azeri manat.  National accounts base year: 2000.  National accounts reference year: .  Latest population census: 2009.  Latest household survey: Demographic and Health Survey (DHS), 2006.  Special notes: April 2012 database update: National accounts historical expenditure series in constant prices were revised in line with State Statistical Committee data that were not previously available.",
		"Region": "Europe \u0026 Central Asia",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BEN",
		"ShortName": "Benin",
		"SpecialNotes": "Benin.  Region: Sub-Saharan Africa.  Income group: Low income.  Lending category: IDA.  Currency unit: CFA franc.  National accounts base year: 1985.  National accounts reference year: .  Latest population census: 2013.  Latest household survey: Multiple Indicator Cluster Survey (MICS), 2014.",
		"Region": "Sub-Saharan Africa",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BFA",
		"ShortName": "Burkina Faso",
		"SpecialNotes": "Burkina Faso.  Region: Sub-Saharan Africa.  Income group: Low income.  Lending category: IDA.  Currency unit: CFA franc.  National accounts base year: 1999.  National accounts reference year: .  Latest population census: 2006.  Latest household survey: Malaria Indicator Survey (MIS), 2014.",
		"Region": "Sub-Saharan Africa",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BGD",
		"ShortName": "Bangladesh",
		"SpecialNotes": "Bangladesh.  Region: South Asia.  Income group: Lower middle income.  Lending category: IDA.  Currency unit: Bangladeshi taka.  National accounts base year: 2005/06.  National accounts reference year: .  Latest population census: 2011.  Latest household survey: Demographic and Health Survey (DHS), 2014; HIV/Maternal and Child Health (HIV/MCH) Service Provision Assessments (SPA), 2014.  Special notes: Fiscal year end: June 30; reporting period for national accounts data: FY. The new base year is 2005/06.",
		"Region": "South Asia",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BGR",
		"ShortName": "Bulgaria",
		"SpecialNotes": "Bulgaria.  Region: Europe \u0026 Central Asia.  Income group: Upper middle income.  Lending category: IBRD.  Currency unit: Bulgarian lev.  National accounts base year: Original chained constant price data are rescaled..  National accounts reference year: 2010.  Latest population census: 2011.  Latest household survey: Living Standards Measurement Study Survey (LSMS), 2007.  Special notes: The new reference year for chain linked series is 2010. April 2011 database update: The National Statistical Office revised national accounts data from 1995 onward. GDP in current prices were about 4 percent higher than previous estimates.",
		"Region": "Europe \u0026 Central Asia",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BHS",
		"ShortName": "Bahamas, The",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BIH",
		"ShortName": "Bosnia and Herzegovina",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BLR",
		"ShortName": "Belarus",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BLZ",
		"ShortName": "Belize",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	}
]`,
	},
	{
		columns: []string{"code", "short_name"},
		rows: toArray(`
AFG	Afghanistan123
        `),
		expects: `[
	{
		"Code": "AFG",
		"ShortName": "Afghanistan123",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 12,
		"AvgAge": 99.01
	},
	{
		"Code": "AGO",
		"ShortName": "Angola",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 13,
		"AvgAge": 99.02
	},
	{
		"Code": "ALB",
		"ShortName": "Albania",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 14,
		"AvgAge": 99.09
	},
	{
		"Code": "ARG",
		"ShortName": "Argentina",
		"SpecialNotes": "Argentina.  Region: Latin America \u0026 Caribbean.  Income group: High income: nonOECD.  Lending category: IBRD.  Currency unit: Argentine peso.  National accounts base year: 2004.  National accounts reference year: .  Latest population census: 2010.  Latest household survey: Multiple Indicator Cluster Survey (MICS), 2011/12.  Special notes: The base year has changed to 2004.",
		"Region": "Latin America \u0026 Caribbean",
		"IncomeGroup": "",
		"Count": 15,
		"AvgAge": 99.04
	},
	{
		"Code": "ARM",
		"ShortName": "Armenia",
		"SpecialNotes": "Armenia.  Region: Europe \u0026 Central Asia.  Income group: Lower middle income.  Lending category: IBRD.  Currency unit: Armenian dram.  National accounts base year: Original chained constant price data are rescaled..  National accounts reference year: 1996.  Latest population census: 2011.  Latest household survey: Demographic and Health Survey (DHS), 2010.",
		"Region": "Europe \u0026 Central Asia",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "AZE",
		"ShortName": "Azerbaijan",
		"SpecialNotes": "Azerbaijan.  Region: Europe \u0026 Central Asia.  Income group: Upper middle income.  Lending category: IBRD.  Currency unit: New Azeri manat.  National accounts base year: 2000.  National accounts reference year: .  Latest population census: 2009.  Latest household survey: Demographic and Health Survey (DHS), 2006.  Special notes: April 2012 database update: National accounts historical expenditure series in constant prices were revised in line with State Statistical Committee data that were not previously available.",
		"Region": "Europe \u0026 Central Asia",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BEN",
		"ShortName": "Benin",
		"SpecialNotes": "Benin.  Region: Sub-Saharan Africa.  Income group: Low income.  Lending category: IDA.  Currency unit: CFA franc.  National accounts base year: 1985.  National accounts reference year: .  Latest population census: 2013.  Latest household survey: Multiple Indicator Cluster Survey (MICS), 2014.",
		"Region": "Sub-Saharan Africa",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BFA",
		"ShortName": "Burkina Faso",
		"SpecialNotes": "Burkina Faso.  Region: Sub-Saharan Africa.  Income group: Low income.  Lending category: IDA.  Currency unit: CFA franc.  National accounts base year: 1999.  National accounts reference year: .  Latest population census: 2006.  Latest household survey: Malaria Indicator Survey (MIS), 2014.",
		"Region": "Sub-Saharan Africa",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BGD",
		"ShortName": "Bangladesh",
		"SpecialNotes": "Bangladesh.  Region: South Asia.  Income group: Lower middle income.  Lending category: IDA.  Currency unit: Bangladeshi taka.  National accounts base year: 2005/06.  National accounts reference year: .  Latest population census: 2011.  Latest household survey: Demographic and Health Survey (DHS), 2014; HIV/Maternal and Child Health (HIV/MCH) Service Provision Assessments (SPA), 2014.  Special notes: Fiscal year end: June 30; reporting period for national accounts data: FY. The new base year is 2005/06.",
		"Region": "South Asia",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BGR",
		"ShortName": "Bulgaria",
		"SpecialNotes": "Bulgaria.  Region: Europe \u0026 Central Asia.  Income group: Upper middle income.  Lending category: IBRD.  Currency unit: Bulgarian lev.  National accounts base year: Original chained constant price data are rescaled..  National accounts reference year: 2010.  Latest population census: 2011.  Latest household survey: Living Standards Measurement Study Survey (LSMS), 2007.  Special notes: The new reference year for chain linked series is 2010. April 2011 database update: The National Statistical Office revised national accounts data from 1995 onward. GDP in current prices were about 4 percent higher than previous estimates.",
		"Region": "Europe \u0026 Central Asia",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BHS",
		"ShortName": "Bahamas, The",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BIH",
		"ShortName": "Bosnia and Herzegovina",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BLR",
		"ShortName": "Belarus",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	},
	{
		"Code": "BLZ",
		"ShortName": "Belize",
		"SpecialNotes": "",
		"Region": "",
		"IncomeGroup": "",
		"Count": 0,
		"AvgAge": 0
	}
]`,
	},
}

func toArray(data string) (r [][]interface{}) {
	rd := csv.NewReader(strings.NewReader(data))
	rd.Comma = '\t'
	for {
		records, err := rd.Read()
		if err != nil {
			return
		}
		row := []interface{}{}
		for _, field := range records {
			row = append(row, strings.TrimSpace(field))
		}
		r = append(r, row)
	}
	return
}

func openAndMigrate() *gorm.DB {
	d, err := gorm.Open(os.Getenv("DB_DIALECT"), os.Getenv("DB_PARAMS"))
	d.DropTable(&Country{})
	d.AutoMigrate(&Country{})
	d.LogMode(true)

	if err != nil {
		panic(err)
	}
	return d
}

func TestPut(t *testing.T) {
	db := openAndMigrate()
	for _, c := range cases {
		err := batchputs.Put(db.DB(), os.Getenv("DB_DIALECT"), "countries", "code", c.columns, c.rows)
		if err != nil {
			panic(err)
		}

		var cs []Country
		err = db.Order("code ASC").Find(&cs).Error
		if err != nil {
			panic(err)
		}
		diff := testingutils.PrettyJsonDiff(c.expects, cs)
		if len(diff) > 0 {
			t.Error(diff)
		}
	}
}
