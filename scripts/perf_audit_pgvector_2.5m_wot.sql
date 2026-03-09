-- ==============================================================================
-- POSTGRESQL + PGVECTOR PERFORMANCE INVESTIGATION SCRIPTS
-- ==============================================================================
-- This script contains queries designed to execute in DBeaver (or any SQL client)
-- against a CloudNativePG PostgreSQL instance with pgvector. It facilitates
-- understanding of regular vector search, filtered vector search, and hybrid search.
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 1. DATABASE EXPLORATION & SAMPLING
-- ------------------------------------------------------------------------------

-- 1.1. Basic Sample
-- Retrieve a basic sample of rows to understand the table schema.
SELECT * FROM wot_chunks_2_5m LIMIT 10;

-- 1.2. Metadata Distribution
-- Analyze the distribution of data across different 'books'. This helps
-- identify the selectivity of our metadata filters.
SELECT
    metadata->>'book_name' AS book_name,
    COUNT(*) AS chunk_count
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' IS NOT NULL
GROUP BY metadata->>'book_name'
ORDER BY chunk_count DESC;

-- 1.3. Sample Vector Extraction
-- Extract a sample vector from a specific book to use as our query vector
-- in subsequent search queries.
SELECT
    metadata->>'book_name' AS sample_book,
    embedding::text AS sample_vector
FROM wot_chunks_2_5m
WHERE
    metadata->>'book_name' IS NOT NULL
    AND metadata->>'book_name' = '00. New Spring'
LIMIT 50;

-- ------------------------------------------------------------------------------
-- 2. QUERY CONTEXT SETUP
-- ------------------------------------------------------------------------------

-- Set a session variable with the sample embedding extracted above.
-- This allows us to reuse the same vector in multiple queries without
-- pasting the huge array every time.
SET
myvars.embedding = '[-0.008212566,-0.0048901504,-0.0056281006,-0.007248312,0.0054837903,0.019534364,-0.028914532,-0.020006651,0.013880025,-0.026526852,0.026762996,0.016359538,0.0069268933,-0.018655382,0.00095031597,0.031066066,0.03754691,0.014680291,0.015349366,0.0018743937,-0.000904399,0.003919336,0.0074910154,0.011512024,0.012935448,-0.006913774,0.023706242,-0.024440913,0.01528377,-0.047858536,0.014168645,0.004408023,-0.01974427,0.011774407,-0.043529227,-0.012338528,0.027051616,-0.024349079,0.024965677,-0.021397278,0.01440479,0.003456887,-0.011702252,-0.013407736,-0.027051616,0.005726494,0.03576271,-0.0072942283,-0.020715084,0.01974427,0.018576669,-0.010344423,-0.009012832,-0.015414962,-0.023129001,0.02269607,-0.013099438,0.019455649,0.020059127,-0.0013881664,-0.016136512,0.009255536,-0.0064808433,-0.01942941,-0.00024680336,-0.0075303726,-0.00816009,0.0075762896,-0.027681334,-0.020124724,0.022879738,0.009937731,0.008868522,-0.0057822503,0.028179862,-0.015821654,0.010285387,-0.010547769,-0.039829634,-0.008553663,0.0139456205,-0.023627527,-0.030698732,0.032902744,0.0084224725,-0.021725256,0.0014414629,0.019101433,-0.030672492,-0.011623537,0.015152579,-0.00024516348,-0.011512024,0.03668105,-0.0038930976,-0.008993154,0.003427369,0.024585223,0.0069793696,-0.013696357,0.0035060837,-0.0027418952,0.00010895016,0.00326502,-0.015611748,-0.0133487005,-0.011170927,0.008645497,0.025293656,-0.015178817,-0.0016808867,0.04019697,0.021620303,0.002609064,-0.025411727,-0.03592014,-0.006736666,-0.009898373,0.008868522,-0.0068809763,-0.00063996686,0.009675348,-0.017868236,-0.0059068818,0.016307062,0.028206099,-0.0027107373,0.0005551026,-0.0022089311,-0.011525143,-0.018681621,0.017382829,0.029308105,-0.015716702,0.004627768,0.017750164,-0.026631806,-0.0012528755,0.001201219,-0.009662229,-0.009550717,0.014496624,-0.025608514,0.008625818,-0.0021023382,0.019416291,0.017172923,-0.0040177293,-0.0014775405,0.00069613307,0.037232053,-0.004394904,0.024270365,-0.0030682331,-0.012784579,-0.0027796128,-0.020557655,0.018904647,-0.019403173,-0.022945333,0.0131191155,0.015519914,0.021948282,-0.016753111,0.032377977,0.01587413,0.010200113,0.009472001,-0.015414962,-0.013158473,-0.019731151,-0.0061528655,-0.0054936297,0.009163703,-0.015821654,0.023758719,0.028363528,-0.00074164005,-0.016254585,-0.00979998,-0.0237456,-0.015454318,0.04245346,0.0039226157,-0.011708811,-0.0042177956,0.0058872034,-0.014076811,-0.0037553469,0.00036856518,0.0024516347,-0.0044768983,-0.010114838,-0.012968246,-0.66078365,-0.03298146,-0.02076756,-0.012003991,0.0029436017,0.0001735208,-0.0021761335,0.0080551375,0.0030354355,0.009229298,-0.005401796,0.020347748,-0.019048957,-0.0074778963,-0.0068940953,-0.0066973087,0.004545774,-0.010482173,0.0023106043,-0.0046671256,-0.021554707,0.009937731,0.007182716,-0.009695027,0.006802262,-0.020859394,0.01886529,-0.0025910253,-0.014614695,0.02093811,-0.026658043,0.025346132,-0.009596634,0.0149295535,0.05562505,-0.033375032,-0.016228346,0.022499284,0.019167028,0.0122270165,-0.013893144,-0.015808534,0.006215181,0.009012832,0.019704912,0.0049065496,0.032036882,-0.012286052,-0.0075303726,-0.023653766,0.014365432,0.017290995,-0.008350317,0.009616312,-0.0017300834,0.002712377,0.037651863,-0.014562218,0.011262761,0.006782583,-0.009117786,0.030095251,-0.022473045,-0.006736666,-0.0012495958,-0.004732721,0.011289,-0.0024745932,0.014903315,-0.015926607,0.033873558,0.035710234,0.009380168,0.027628858,0.009806539,0.010482173,0.05147941,0.00027283662,-0.00053624384,-0.01736971,0.00090767886,-0.0079173865,-0.020518297,-0.010344423,0.05239775,-0.0054673916,-0.006369331,0.01068552,-0.00326174,0.003174826,-0.009767182,0.028573435,-0.008212566,-0.011885919,0.009124345,0.0151132215,0.016831826,0.0051754913,0.029150676,-0.03710086,0.017474663,-0.013538928,0.049931355,0.0022646873,0.020137843,0.0045916904,0.0018973522,-0.007523813,0.015651105,-0.035998855,0.017304113,0.01942941,-0.018904647,-0.014942673,0.0014045653,-0.020347748,0.009170262,0.012968246,0.02137104,-0.03828158,0.014798363,-0.00042391144,0.0040636463,-0.00682194,0.014312956,0.021567827,0.0015119781,-0.0068481783,-0.0027517346,-0.007950184,0.0044572195,0.009242417,0.032141834,-0.010016445,0.016123394,-0.014089931,0.03825534,-0.0062381397,0.035815187,-0.012358207,-0.0022581278,0.016307062,0.0028091306,-0.015716702,0.0009109586,-0.032561645,-0.010619924,-0.0045162556,-0.021777732,0.013414296,0.006782583,0.0035683995,-0.011794086,-0.014850839,-0.0059331204,-0.004660566,-0.025188703,0.0052935635,-0.0074516577,-0.011367714,0.019088313,0.016412014,-0.030095251,0.019678675,-0.0020088647,-0.015834773,0.0007998561,0.00712368,0.012410684,-0.03235174,-0.00021564547,-0.014877077,0.0013693077,0.045129757,-0.0060675913,-0.010121398,-0.024112934,0.0020219837,-0.005267325,0.0058872034,-0.009655669,0.004362106,-0.0023319228,-0.0028714465,0.014260479,0.00058093085,0.015467438,0.020531416,-0.0046244883,0.009688467,-0.0069990484,-0.003919336,-0.01113157,0.0060216743,-0.00041243221,-0.0033650533,0.031958167,0.01691054,0.0015398562,0.030016538,-0.00020160392,-0.015769178,0.020426463,-0.0049819844,0.0043260283,-0.041194025,0.007136799,-0.02776005,0.028101146,-0.004480178,0.0057822503,-0.037599385,-0.020242795,-0.0058609652,0.0015218174,0.026172636,0.008927559,0.014759005,-0.0025008314,-0.012987925,-0.009124345,-0.019678675,0.021777732,-0.00041345714,-0.0018350363,0.011072534,0.044604994,-0.0069400123,0.002728776,-0.008697974,0.0016333299,0.03222055,-0.020019772,0.011498905,0.008278162,0.012830496,0.011879359,-0.018432358,0.03413594,-0.027182808,-0.0044178623,0.018484835,0.040721737,-0.010639603,0.008829165,0.022538641,0.018065022,0.003381452,-0.010567448,0.0013898064,0.02612016,-0.0054805106,0.005296843,-0.008960356,0.014968911,-0.019665554,0.010062362,0.0017005655,0.007163037,0.013001044,0.018629145,0.013473332,0.0016595682,-0.020282153,0.028153623,-0.0012823936,0.0096359905,-0.017828878,-0.023614408,-8.542799e-05,-0.013866905,0.005510029,0.03620876,0.0063594915,0.043319322,0.012246694,-0.011557941,0.007661564,0.02833729,0.010403459,-0.02802243,-0.035185467,0.03027892,0.018156856,-0.007858351,-0.014103049,-0.019416291,0.034188416,-0.01840612,0.016871182,-0.0019908259,-0.003161707,-0.0007773076,-0.0073401453,0.030042775,-0.001943269,0.01869474,-0.01883905,0.010173874,-0.012174539,-0.011407072,-0.025988968,-0.010508412,-6.4314418e-06,0.027917478,0.0044572195,-0.0049623055,-0.018484835,-0.00667763,-0.0076353257,-0.01513946,-0.008861963,-0.0010954462,-0.022787904,0.0054837903,-0.007923946,-0.0011167647,0.00017680059,0.039226156,0.0042112363,0.014194883,-0.014995149,-0.009859015,0.002330283,0.12678313,0.013683238,-0.0032978177,-0.0097737415,0.014968911,-0.008835725,-0.01793383,-0.050377406,0.007582849,-0.0034962443,0.013696357,-0.013565166,-0.009071869,0.013001044,0.02699914,-0.0103969,-0.014562218,-0.009813099,-0.0013192911,-0.006057752,0.0155723905,-0.016648158,0.0004349807,-0.0049688653,-0.02550356,-0.037783053,-0.0069268933,0.014772125,-0.020387106,0.0014627815,0.0013857066,0.0056740176,0.007674683,0.012699304,-0.018091261,-0.008232245,-0.009872135,-0.005503469,0.015664224,-0.031826977,0.028206099,0.0018432358,0.010200113,-0.008415913,0.0059527988,-0.010783914,-0.014772125,0.016241465,-0.0026353025,-0.019849222,0.047727343,0.014352313,-0.014286717,0.00017321332,0.008638938,0.029439297,-0.0006867037,0.008809486,-0.017868236,0.055152763,-0.023443861,-0.010665841,0.018183095,-0.008363436,-0.009760622,-0.024086697,-0.0050082225,-0.0036438345,-0.02509687,0.0074385386,-0.012863293,-0.012961687,-0.008868522,-0.02152847,0.022630475,0.024900082,-0.0066579515,-0.014168645,0.0049032695,-0.00022179505,0.0012856734,-0.0049163885,0.0031108703,-0.009144024,-0.015821654,0.004647447,-0.017868236,0.011282439,-0.000682604,-0.011393952,-0.002789452,0.013617642,0.0043293084,-0.015637986,0.019088313,-0.002299125,-0.020688847,0.02907196,-0.010206672,0.010967581,0.014024335,-0.01898336,0.00081830483,-0.006900655,0.019062076,-0.02535925,0.0054345937,0.010770794,0.0021056181,-0.005270605,-0.022315616,-0.030777445,0.008409353,-0.0008470029,-0.022145068,-0.0034503276,0.014116169,0.034660704,0.020492058,-0.02121361,-0.0031387485,-0.017461544,0.016018441,0.01883905,-0.030672492,0.015414962,0.017474663,-0.012522196,-0.0052214083,0.0064808433,0.015861012,0.017697688,0.0034503276,0.006500522,-0.041298978,-0.015506795,-0.0011659614,-0.003401131,-0.003353574,-0.0045687323,0.006516921,-0.02076756,-0.0175009,-0.028730864,0.0076025277,-0.017133566,-0.013309343,0.01767145,-0.017015493,0.015218174,-0.028599672,-0.008566783,-0.011361155,-0.002935402,-0.0045359344,-0.042217314,0.008579901,-0.0119515145,0.029649202,0.021187373,0.022932215,-0.016700635,0.0060019954,-0.0038078234,-0.01855043,-0.009176821,-0.0027681335,-0.018484835,-0.03830782,0.009550717,0.02773381,0.007156478,0.016451372,-0.005264045,-0.0012454961,0.017277876,-0.006041353,-0.014155526,-0.013066639,-0.027576381,-0.013237188,-0.010547769,-0.009353929,0.0030370753,-0.022341855,-0.020701965,0.03948854,0.003978372,0.014365432,-0.022131948,0.037441958,0.009071869,0.0152968895,0.008704534,-0.004811436,-0.016241465,0.010488734,0.004348987,0.018222451,0.016884303,0.023719361,0.011610418,-0.0023401224,-0.0096359905,-0.009950849,0.004253873,-0.023378264,-0.009668789,0.0059560784,-0.024007982,-0.02093811,-0.02776005,-0.013840667,-0.0100492425,0.003427369,-0.017159803,-0.018025665,-0.0030059174,-0.02108242,-0.029859107,-0.0137619525,-0.010797032,0.024493389,0.003607757,0.028704626,0.006116788,-0.015900368,-0.027681334,0.01157762,0.01705485,-0.00133651,0.006215181,-0.011944955,0.0016554685,0.016110275,-0.007595968,0.0067891423,-0.020885633,-0.03557904,0.01572982,0.021620303,0.0020121443,-0.018930884,-0.015414962,-0.011643215,0.0103969,-0.02240745,0.0033174965,0.0013939061,-0.017146684,-0.0061922227,0.0069400123,-0.0042341948,0.016307062,-0.0024139173,0.004519535,0.007897708,-0.026448138,-0.007609087,-0.0017038452,-0.0038471806,0.026303828,-0.012797697,0.0043030703,0.00096179516,0.0069793696,-0.014063693,-0.018720979,-0.017330352,0.023378264,-0.012889531,-0.013388058,0.02179085,-0.011223404,-0.0053230813,0.0037356683,-0.00028862056,0.0038766987,-0.01439167,-0.005598583,0.031328447,0.001810438,-0.0029436017,-0.008199448,-0.029124437,-0.010764235,-0.017986309,-0.003071513,0.0137619525,0.00038393913,-0.009399846,0.003768466,-0.01676623,0.008120732,0.00078591704,0.009170262,-0.017330352,0.022630475,-0.040905405,0.012436922,-0.009006273,0.010901986,-0.0008486428,0.0014775405,0.010108279,-0.0094982395,0.014076811,-0.008934118,-0.014089931,-0.0047884774,0.012981365,0.0047425604,-0.012823936,0.00370943,-0.0035946378,0.006297176,0.013407736,-0.010567448,-0.010259149,0.010895425,0.009419525,0.0024680337,0.022643594,-0.031564593,0.0023778398,0.0069728103,0.018655382,0.00041202223,0.0041522,-0.0113545945,0.009412966,0.0064775636,-0.019324457,-0.042663366,-0.011780966,-0.0081469705,-0.005165652,0.008704534,0.011918717,-0.0047819177,0.025464203,-0.01807814,-0.02164654,-0.009084987,-0.022709189,-0.015651105,0.016215228,0.00519845,-0.019324457,-0.035972618,-0.0075041344,0.033637412,0.009576955,-0.02878334,-0.017553378,0.010882307,-0.013040401,-0.00029169535,-0.0044309814,0.023247074,0.015821654,0.0014529421,-0.0065202005,0.04077421,-0.0011610418,-0.0010724877,0.0059527988,-0.020570774,-0.028967008,-0.016831826,0.009183381,0.025923373,-0.025018154,0.011387393,0.0045031365,-0.00474584,-0.000994593,0.01275178,-0.024270365,-0.008219126,-0.012843614,-0.013407736,0.010495293,-0.0011987592,-0.037337005,-0.009321132,-0.015349366,-0.007818993,0.008448711,-0.0140374545,-0.013237188,0.0009478561,0.013158473,-0.009996766,-0.012292611,0.0023352026,-0.0027812526,-0.0380192,0.026526852,-0.008704534,-0.023076525,0.011505465,-0.01827493,-0.026054565,-0.00040033803,0.004293231,-0.0037356683,-0.0062807766,0.001097906,-0.03334879,0.011085653,0.018720979,0.01719916,-0.008258483,0.00025930753,0.0077861953,0.01778952,-0.009111226,0.01052809,0.0145491,-0.008671735,0.00090849877,-0.012351648,-0.0062283003,0.016372656,-0.022879738,-0.024152292,-0.0053230813,-0.014116169,-0.00563794,-0.0012750141,-0.02479513,0.02594961,-0.024388436,-0.019271981,0.0030600338,0.23719361,-0.03426713,-0.00072606106,0.017356591,-0.025201822,0.005418195,0.01380131,-0.008553663,0.0029042442,0.0081010545,-0.009990207,0.017540257,-0.0069268933,-0.001766161,0.005605142,-0.032561645,-0.027182808,-0.002745175,-0.0109807,-0.03455575,-0.0023352026,-0.009990207,-0.021659661,-0.0120433485,0.013788191,0.020662608,-0.022709189,0.0038209425,-0.009111226,-0.003473286,-0.009918052,-0.028101146,-0.017409068,-0.008730772,-0.022564879,0.0036700726,0.020557655,0.020715084,-0.022905976,0.0010159116,0.012673066,0.0039127762,0.0024827926,0.0061135083,0.0097737415,0.025779063,-0.0015669144,-0.010665841,-0.018235572,0.0066973087,-0.03056754,-0.009458883,0.022131948,0.027917478,-0.010514972,-0.0015988923,0.01143331,0.014024335,-0.015008269,-0.010068921,-0.010055803,-0.005372278,0.0030961114,0.023561932,-0.011958074,0.046074335,-0.020085366,-0.009412966,-0.006205342,-0.00534604,-0.011741608,0.002951801,-0.00444738,0.008127293,-0.0335587,-0.01824869,0.01767145,0.003604477,-0.016687516,0.009747503,-0.009596634,-0.0134864515,0.003456887,-0.0010560888,-0.005309962,-0.026762996,0.018970242,0.0055592256,-0.03012149,0.015821654,-0.008848844,-0.023247074,-0.008520866,-0.00045137957,0.012338528,-0.017094208,0.017422186,0.021712137,0.010915104,-0.011485786,0.00578553,-0.04526095,-0.015073864,-0.005398516,-0.015677344,0.016844945,-0.004080045,0.0013233909,0.010331304,-0.002923923,1.19596625e-05,-0.02922939,0.007327026,-0.009747503,0.00017485322,-0.0135782845,0.002120377,-0.0004985264,0.015349366,-0.03990835,-0.0073795025,-0.0020219837,0.00031342387,0.020150961,0.0138537865,-0.02328643,-0.044263896,0.0016234906,0.017041732,-0.01706797,-0.0030682331,-0.027209047,0.013525808,0.0096359905,-0.0010610085,0.022709189,0.028179862,-0.00222697,-0.0063988487,0.0021892525,0.021134896,0.01157762,0.0317745,0.018668503,0.030672492,-0.0068350593,0.033322554,-0.007858351,-0.042374745,-0.0058740843,-0.029019484,0.003578239,-0.00087652093,-0.0030928315,0.03324384,-0.0016726872,-0.023207717,-0.04082669,-0.0007781276,0.021620303,-0.022512402,0.021239849,0.01765833,-0.008999714,0.00056166213,0.0145491,-0.16708507,0.02388991,-6.984904e-05,-0.015677344,0.019626198,-0.012036789,0.011026617,-0.0060151145,-0.039829634,-0.02462458,0.009163703,-0.005532987,-0.011761287,0.025897134,0.008730772,0.030515064,-0.006172544,0.004184998,0.022814143,0.018616026,0.0335587,-0.026028326,-0.0011946595,-0.0101869935,0.021279206,-0.01067896,-0.010593686,0.019062076,-0.020190319,-0.009045631,-0.013866905,0.00341425,0.017317234,0.00920962,0.03972468,0.0049688653,-0.015467438,0.006808821,-0.0029600004,0.031564593,0.0070318463,0.016398896,0.020964347,0.015533033,-0.00222369,0.021331683,-0.0041882778,-0.0029632803,0.010259149,-0.031118544,-0.019403173,-0.02596273,0.013315903,-0.011315238,0.025857778,0.026146399,0.023666885,0.0069268933,0.011380833,-0.0023680003,-0.01781576,-0.017776402,0.0013865266,-0.018196214,-0.013309343,-0.004880311,-0.016398896,0.018038785,-0.012942008,-0.0004554793,-0.020715084,-0.007923946,0.00022138508,-0.030672492,0.030095251,-0.0037061502,-0.009937731,0.01306008,0.0012553354,-0.021659661,-0.0049196687,0.030646255,-0.013263426,0.0012249975,0.021108657,0.009117786,0.0069859293,0.013283105,-0.012233576,0.011393952,0.012194218,-0.014798363,-0.020059127,-0.026802354,0.004552333,0.012705863,0.044447564,-0.006874417,0.01558551,0.0023188037,0.007628766,-0.013473332,-0.004093164,0.026631806,0.0057625715,0.0061397464,0.0006440666,0.009399846,0.026487496,-0.01869474,-0.0037028706,0.0020006653,-0.0033453745,0.01351269,-0.008409353,0.0028124105,-0.0029272027,-0.022984691,0.015756058,-0.03164331,0.056359723,0.016674396,-0.012292611,0.013998097,-0.0057789707,-0.004021009,-0.120276056,0.009255536,-0.0026664604,-0.00727455,0.00027386154,0.017986309,-0.015559272,0.009452323,-0.030908637,0.01764521,0.0027369757,-0.013748834,-0.021161133,-4.297023e-05,0.03085616,-0.024349079,0.0016694075,-0.037494432,-0.025805302,0.008940677,-0.013683238,0.009878694,0.0021006984,-0.009176821,-0.0016956457,-0.013814429,-0.010016445,0.016307062,0.012712424,0.014312956,-0.009170262,-0.007779636,0.011118451,-0.0070187273,-0.005949519,-0.012154861,-0.034791894,-0.012174539,0.021423517,-0.002033463,0.01677935,0.02521494,-0.007163037,-0.037651863,-0.0044900174,0.008219126,-0.0036733525,0.02907196,0.012345088,-0.0031961447,-0.014076811,-0.01646449,-0.038543962,0.013893144,0.025752824,0.0043030703,0.005218128,0.028311051,-0.017461544,0.007687802,-0.0023614408,-0.0076222066,0.017697688,0.0057396134,-0.016753111,-0.014877077,-0.023037167,0.007372943,0.0045096963,0.0057199346,-0.0020531416,0.034240894,-0.019271981,0.013388058,-0.025608514,0.0059364,-0.01172849,0.0020744603,0.012679625,-0.009596634,-0.0056936964,-0.03145964,0.016989255,-0.034791894,0.017527139,0.023194596,-0.00069162337,-0.016202109,0.016582562,0.0009388367,-0.008520866,0.00979342,0.021108657,-0.008442151,-0.014955793,0.0059757573,-0.011013498,-0.0011225044,-0.00756973,0.0014734407,-0.0020137844,0.013827548,-0.042847034,0.028914532,0.0086127,-0.00031383385,-0.0018448756,-0.013224069,0.0084224725,-0.02507063,-0.006329973,0.018773455,-0.002404078,0.03382108,0.004627768,0.00741886,-0.020898752,-0.013020722,0.008822605,-0.00052271475,0.0017579616,0.02076756,-0.0055264276,0.003965253,-0.01171537,0.0028091306,-0.013880025,0.022394331,-0.0120892655,0.013775072,-0.004850793,-0.0030813524,0.029334344,-0.025149345,-0.0030370753,-0.00047884774,-0.016385775,-0.018340524,0.008652057,0.011689132,0.0030993912,0.040433116,-0.020426463,0.0038111033,0.019836104,-0.017120447,-0.010633043,0.005211569,-0.008638938,0.02465082,0.035683997,0.003994771,-0.017684568,0.026395662,-0.01588725,0.004690084,-0.02179085,-0.025595395,0.03101359,-0.0032240227,0.0016579282,-0.02951801,0.033899795,0.014509742,0.0019235903,0.0023056846,0.023640648,0.0070974417,0.0066710706,0.009098107,0.0114923455,-0.01349957,-0.031275973,-0.0051722117,-0.009150583,-0.019560602,0.004086605,0.008763569,0.016136512,-0.009012832,-0.010895425,0.006316854,0.008002661,-0.0011692411,-0.0121351825,0.007222073,-0.0059068818,0.026028326,0.0034962443,0.0050016628,-0.0033027374,-0.0025483882,0.00014677011,0.01813062,-0.007818993,0.00021093078,0.016438251,0.012712424,-0.021397278,0.036864717,0.029885346,0.0026451417,0.0050213416,-0.0012996125,0.0073532644,0.0049065496,-0.016844945,0.0019219505,-0.03592014,-0.009084987,-0.013735714,0.005910162,-0.011990872,-0.002996078,0.019376934,0.014903315,-0.01364388,0.0057494524,0.020885633,-0.016084036,-0.017474663,0.0012815737,0.024519628,0.023994863,-0.011603858,-0.021134896,-0.00012985874,-0.0010962661,0.002253208,0.0064021284,-0.0012036789,0.0034175296,0.026474375,-0.01974427,0.0023581611,-0.01646449,0.008835725,-0.007523813,-0.005211569,0.01840612,-0.013020722,0.02522806,0.024060458,-0.003219103,0.03206312,0.011636656,0.035421614,-0.018602906,0.021843327,-0.0065825162,-0.011518584,0.015808534,0.0013971858,0.014181764,-0.030436348,-0.034608226,0.029255629,0.012922329,-0.009157143,0.000519025,-0.016097154,0.029308105,0.0019399893,0.014968911,-0.013709476,-0.031826977,-0.0036963108,0.023903029,0.0008593021,0.022669833,-0.0021728536,0.0075762896,0.014968911,-0.025713468,-0.012076146,0.021489112,-0.026776116,-0.010783914,-0.008816046,0.019206386,-0.00637589,-0.024073578,0.024099816,-0.011610418,-0.009734384,0.013880025,0.005211569,-0.017159803,0.018497953,-0.007956744]
'


-- Set a session variable for the text search keyword (used in Hybrid Search).
SET myvars.keyword = 'foretelling';

-- (Optional) Experiment with pgvector HNSW iterative scan settings.
-- strict_order: guarantees exact distance ordering (can be slower with filters)
-- relaxed_order: allows some relaxation for better performance
-- off: disables iterative scan
-- SET hnsw.iterative_scan = strict_order;
-- SET hnsw.iterative_scan = relaxed_order;
-- SET hnsw.iterative_scan = off;

-- Set ef_search to at least match your LIMIT (or higher for better recall)
SET hnsw.ef_search = 100;

-- ------------------------------------------------------------------------------
-- 3. SCENARIO A: PURE VECTOR SEARCH
-- ------------------------------------------------------------------------------

-- Use EXPLAIN (ANALYZE, BUFFERS) to see the execution plan.
-- This query performs a simple K-Nearest Neighbors (KNN) search over the entire
-- dataset without any filters. It should ideally use an Index Scan on the HNSW index.
EXPLAIN (ANALYZE, BUFFERS)
SELECT
    id,
    1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 100;

-- Actual Query Configuration:
SELECT
    id,
    1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity,
    content,
    metadata
FROM wot_chunks_2_5m
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 100;


-- ------------------------------------------------------------------------------
-- 4. SCENARIO B: VECTOR SEARCH + METADATA FILTERING (FILTERED SEARCH)
-- ------------------------------------------------------------------------------

-- This scenario combines a vector similarity search with a metadata hard filter.
-- The query planner has to decide whether to use the vector index, the metadata
-- index, or a combination. Adjusting LIMIT (top_k) can drastically change the plan!
EXPLAIN (ANALYZE, BUFFERS)
SELECT
    id,
    1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '13. Towers of Midnight'
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 38;

-- Actual Query Configuration:
SELECT
    id,
    1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity,
    content,
    metadata
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '13. Towers of Midnight'
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 38;


-- ------------------------------------------------------------------------------
-- 5. SCENARIO C: HYBRID SEARCH (VECTOR + FULL-TEXT SEARCH)
-- ------------------------------------------------------------------------------

-- Hybrid search combines semantic vector similarity with lexical full-text search
-- using Reciprocal Rank Fusion (RRF). This is usually implemented via CTEs (Common
-- Table Expressions).

-- Use EXPLAIN (ANALYZE, BUFFERS) to observe the planner evaluating multiple CTEs
-- and performing the FULL OUTER JOIN.
EXPLAIN (ANALYZE, BUFFERS)
WITH vector_search AS (
  -- 1. Retrieve top candidates using semantic vector search
  SELECT
    id,
    ROW_NUMBER() OVER (ORDER BY embedding <=> current_setting('myvars.embedding')::vector) AS rank,
    1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
  FROM wot_chunks_2_5m
  LIMIT 200 -- Fetch more candidates typically (e.g., top_k * 2)
),
text_search AS (
  -- 2. Retrieve top candidates using lexical full-text search (BM25/ts_rank)
  SELECT
    id,
    ROW_NUMBER() OVER (ORDER BY ts_rank(to_tsvector('english', content), plainto_tsquery('english', current_setting('myvars.keyword'))) DESC) AS rank,
    ts_rank(to_tsvector('english', content), plainto_tsquery('english', current_setting('myvars.keyword'))) AS similarity
  FROM wot_chunks_2_5m
  WHERE to_tsvector('english', content) @@ plainto_tsquery('english', current_setting('myvars.keyword'))
  LIMIT 200
),
rrf_fusion AS (
  -- 3. Combine results using Reciprocal Rank Fusion
  -- The constant 60 is a standard RRF tuning parameter
  SELECT
    COALESCE(v.id, t.id) AS id,
    COALESCE(1.0 / (60 + v.rank), 0.0) + COALESCE(1.0 / (60 + t.rank), 0.0) AS rrf_score,
    v.similarity AS vector_similarity,
    t.similarity AS text_similarity
  FROM vector_search v
  FULL OUTER JOIN text_search t ON v.id = t.id
)
-- 4. Return the final sorted results
SELECT
    f.id,
    f.rrf_score,
    f.vector_similarity,
    f.text_similarity
FROM rrf_fusion f
ORDER BY f.rrf_score DESC
LIMIT 100;

-- Actual Query Configuration:
WITH vector_search AS (
  SELECT
    id,
    ROW_NUMBER() OVER (ORDER BY embedding <=> current_setting('myvars.embedding')::vector) AS rank
  FROM wot_chunks_2_5m
  LIMIT 82
),
text_search AS (
  SELECT
    id,
    ROW_NUMBER() OVER (ORDER BY ts_rank(to_tsvector('english', content), plainto_tsquery('english', current_setting('myvars.keyword'))) DESC) AS rank
  FROM wot_chunks_2_5m
  WHERE to_tsvector('english', content) @@ plainto_tsquery('english', current_setting('myvars.keyword'))
  LIMIT 82
),
rrf_fusion AS (
  SELECT
    COALESCE(v.id, t.id) AS id,
    COALESCE(1.0 / (60 + v.rank), 0.0) + COALESCE(1.0 / (60 + t.rank), 0.0) AS rrf_score
  FROM vector_search v
  FULL OUTER JOIN text_search t ON v.id = t.id
)
SELECT
    f.id,
    f.rrf_score,
    w.content,
    w.metadata
FROM rrf_fusion f
JOIN wot_chunks_2_5m w ON f.id = w.id
ORDER BY f.rrf_score DESC
LIMIT 41;


-- ==============================================================================
-- 6. DEEP-DIVE METADATA PROFILING & DATA SKEW
-- ==============================================================================
-- Objective: Map the full cardinality and distribution of the JSONB metadata
-- columns (book_name, chapter_number, chapter_title) to understand filter
-- selectivity before running any planner threshold sweeps.

-- 6.1. Book-Level Distribution
-- For each book: row count, % of dataset, chapter range, and distinct chapter/title counts.
-- Use this to choose representative filters for the planner sweep in Section 8.
SELECT
    metadata->>'book_name'                                             AS book_name,
    COUNT(*)                                                           AS chunk_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 4)                AS pct_of_total,
    MIN((metadata->>'chapter_number')::int)                            AS min_chapter,
    MAX((metadata->>'chapter_number')::int)                            AS max_chapter,
    COUNT(DISTINCT metadata->>'chapter_number')                        AS distinct_chapters,
    COUNT(DISTINCT metadata->>'chapter_title')                         AS distinct_titles
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' IS NOT NULL
GROUP BY metadata->>'book_name'
ORDER BY chunk_count DESC;


-- 6.2. Chapter-Level Distribution Within Each Book
-- Maps every distinct (book_name, chapter_number, chapter_title) triple.
-- Use this to pick specific (book, chapter) pairs for compound-filter experiments (Section 8.3).
SELECT
    metadata->>'book_name'                                             AS book_name,
    (metadata->>'chapter_number')::int                                 AS chapter_number,
    metadata->>'chapter_title'                                         AS chapter_title,
    COUNT(*)                                                           AS chunk_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 6)                AS pct_of_total
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' IS NOT NULL
GROUP BY
    metadata->>'book_name',
    (metadata->>'chapter_number')::int,
    metadata->>'chapter_title'
ORDER BY book_name, chapter_number;


-- 6.3. Total Distinct Combination Count
-- Quick sanity check: how many unique (book, chapter, title) triples exist?
SELECT COUNT(*) AS total_distinct_combos
FROM (
    SELECT DISTINCT
        metadata->>'book_name',
        metadata->>'chapter_number',
        metadata->>'chapter_title'
    FROM wot_chunks_2_5m
    WHERE metadata->>'book_name' IS NOT NULL
) AS combos;


-- 6.4. Data Skew: Top 10 Most-Populated vs Bottom 10 Least-Populated Chapters
-- Identifies which (book, chapter) pairs will give the most vs least selective filters.
-- The least-populated pairs are most likely to trigger early HNSW abandonment.
(
    SELECT
        'top_10_most'                       AS cohort,
        metadata->>'book_name'              AS book_name,
        (metadata->>'chapter_number')::int  AS chapter_number,
        COUNT(*)                            AS chunk_count
    FROM wot_chunks_2_5m
    WHERE metadata->>'book_name' IS NOT NULL
    GROUP BY metadata->>'book_name', (metadata->>'chapter_number')::int
    ORDER BY chunk_count DESC
    LIMIT 10
)
UNION ALL
(
    SELECT
        'bottom_10_least'                   AS cohort,
        metadata->>'book_name'              AS book_name,
        (metadata->>'chapter_number')::int  AS chapter_number,
        COUNT(*)                            AS chunk_count
    FROM wot_chunks_2_5m
    WHERE metadata->>'book_name' IS NOT NULL
    GROUP BY metadata->>'book_name', (metadata->>'chapter_number')::int
    ORDER BY chunk_count ASC
    LIMIT 10
)
ORDER BY cohort, chunk_count DESC;


-- 6.5. Missing / Null Metadata Audit
-- Confirm no rows have partial metadata that could silently skew filter row-count estimates.
SELECT
    COUNT(*)                                                           AS total_rows,
    COUNT(*) FILTER (WHERE metadata IS NULL)                           AS null_metadata,
    COUNT(*) FILTER (WHERE metadata->>'book_name' IS NULL)             AS null_book_name,
    COUNT(*) FILTER (WHERE metadata->>'chapter_number' IS NULL)        AS null_chapter_number,
    COUNT(*) FILTER (WHERE metadata->>'chapter_title' IS NULL)         AS null_chapter_title
FROM wot_chunks_2_5m;


-- ==============================================================================
-- 7. INDEX HEALTH & STORAGE BREAKDOWN
-- ==============================================================================
-- Objective: Understand the on-disk footprint, RAM cost, and actual usage of
-- every index on the table — especially the HNSW vector index.

-- 7.1. All Indexes: Type, Definition, and Disk Footprint
-- Sorted by size descending so the HNSW index appears first.
SELECT
    i.indexname                                              AS index_name,
    i.indexdef                                              AS definition,
    pg_size_pretty(pg_relation_size(c.oid))                 AS index_size_pretty,
    pg_relation_size(c.oid)                                 AS index_size_bytes
FROM pg_indexes i
JOIN pg_class c ON c.relname = i.indexname
WHERE i.tablename = 'wot_chunks_2_5m'
ORDER BY index_size_bytes DESC;


-- 7.2. Table vs. Total Index Size Comparison
-- Shows how much storage each index tier consumes relative to the raw heap.
SELECT
    pg_size_pretty(pg_total_relation_size('wot_chunks_2_5m'))          AS total_with_indexes,
    pg_size_pretty(pg_relation_size('wot_chunks_2_5m'))                 AS heap_only,
    pg_size_pretty(
        pg_total_relation_size('wot_chunks_2_5m')
        - pg_relation_size('wot_chunks_2_5m')
    )                                                                   AS all_indexes_combined;


-- 7.3. Per-Index Usage Statistics
-- idx_scan: times this index was chosen by the planner — low counts reveal unused indexes.
-- idx_tup_read: rows read from the index pages.
-- idx_tup_fetch: rows actually fetched from the heap using index pointers.
SELECT
    s.indexrelname                                          AS index_name,
    s.idx_scan                                              AS times_chosen_by_planner,
    s.idx_tup_read                                          AS index_tuples_read,
    s.idx_tup_fetch                                         AS heap_tuples_fetched,
    pg_size_pretty(pg_relation_size(s.indexrelid))          AS index_size
FROM pg_stat_user_indexes s
WHERE s.relname = 'wot_chunks_2_5m'
ORDER BY s.idx_scan DESC;


-- 7.4. HNSW Graph RAM Overhead Estimate
-- Formula per node (m=16, dim=1536, ~2 average layers):
--   Edge storage : m * 8 bytes per layer * 2 layers = 256 bytes
--   Vector storage: dim * 4 bytes (float32)        = 6,144 bytes
--   Total per node ≈ 6,400 bytes
-- Over 2.5M nodes: ~16 GB  (explains the large work_mem needed during HNSW build).
SELECT
    2500000                                                            AS total_vectors,
    16                                                                 AS m_param,
    1536                                                               AS dimensions,
    pg_size_pretty(2500000 * ((16 * 8 * 2) + (1536 * 4))::bigint)    AS estimated_graph_ram;


-- 7.5. Buffer Cache Hit Rates for Heap and Indexes
-- High hit rates confirm that frequently-accessed index pages are served from shared_buffers.
-- Low rates indicate cold I/O and will inflate query latency for the first runs.
SELECT
    relname                                                            AS table_name,
    heap_blks_read                                                     AS heap_disk_reads,
    heap_blks_hit                                                      AS heap_cache_hits,
    ROUND(
        heap_blks_hit::numeric / NULLIF(heap_blks_hit + heap_blks_read, 0) * 100, 2
    )                                                                  AS heap_cache_hit_pct,
    idx_blks_read                                                      AS index_disk_reads,
    idx_blks_hit                                                       AS index_cache_hits,
    ROUND(
        idx_blks_hit::numeric / NULLIF(idx_blks_hit + idx_blks_read, 0) * 100, 2
    )                                                                  AS index_cache_hit_pct
FROM pg_statio_user_tables
WHERE relname = 'wot_chunks_2_5m';


-- 7.6. Table Bloat & Vacuum Status
-- Dead tuple ratio above ~5% signals the table needs VACUUM ANALYZE before
-- running planner experiments — stale statistics corrupt cost estimates.
SELECT
    n_live_tup                                                         AS live_tuples,
    n_dead_tup                                                         AS dead_tuples,
    ROUND(n_dead_tup::numeric / NULLIF(n_live_tup, 0) * 100, 2)       AS dead_tuple_pct,
    last_vacuum,
    last_autovacuum,
    last_analyze,
    last_autoanalyze
FROM pg_stat_user_tables
WHERE relname = 'wot_chunks_2_5m';


-- 7.7. Cluster Uptime vs. Stats Reset Detection
-- pg_stat_user_indexes counters reset to zero on every PostgreSQL restart,
-- pg_stat_reset() call, or table rebuild. If cluster_uptime is shorter than
-- the time since your last benchmark run, the idx_scan = 0 readings in 7.3
-- are a stats reset artifact — not evidence the index was unused.
SELECT
    pg_postmaster_start_time()                                AS postgres_started_at,
    now() - pg_postmaster_start_time()                        AS cluster_uptime,
    now()                                                     AS checked_at;


-- 7.8. GIN Index Selectivity for the Current Keyword
-- Run this before any hybrid search experiment (Sections 9 and 10.8) to know
-- how many rows the text CTE will need to fetch and score. The selectivity
-- band predicts text CTE latency and tells you whether the hybrid search
-- bottleneck will dominate total query time for this particular keyword.
--
-- Rule of thumb at 2.5M rows, warm cache:
--   < 0.1%  (~2,500 rows)  → text CTE < 100 ms   → hybrid comparable to vector search
--   0.1–1%  (~25,000 rows) → text CTE ~200 ms     → text CTE noticeable but acceptable
--   1–5%    (~125,000 rows)→ text CTE ~500–1,500 ms→ text CTE dominates
--   > 5%    (>125,000 rows)→ text CTE > 1,500 ms  → keyword too common for useful hybrid search
SELECT
    current_setting('myvars.keyword')                         AS keyword,
    COUNT(*)                                                  AS matching_rows,
    ROUND(COUNT(*) * 100.0 / 2500000, 4)                     AS pct_of_total,
    CASE
        WHEN COUNT(*) * 100.0 / 2500000 < 0.1  THEN 'rare    — fast text CTE  (<100 ms)'
        WHEN COUNT(*) * 100.0 / 2500000 < 1.0  THEN 'moderate — acceptable    (~200 ms)'
        WHEN COUNT(*) * 100.0 / 2500000 < 5.0  THEN 'common  — slow text CTE (~1,500 ms)'
        ELSE                                         'very common — text CTE dominates (>1,500 ms)'
    END                                                       AS selectivity_band
FROM wot_chunks_2_5m
WHERE to_tsvector('english', content)
      @@ plainto_tsquery('english', current_setting('myvars.keyword'));


-- 7.9. HNSW Scan Delivery Check — ef_search Ceiling Effect
-- Reveals how many rows the HNSW index actually produces vs. the requested LIMIT.
-- With ef_search = 40 (default) and a cold buffer cache, the HNSW graph scan
-- may exhaust before reaching a large LIMIT (e.g., LIMIT 200 returns only 100 rows).
-- This silently reduces the vector candidate pool in the hybrid search vector CTE,
-- lowering RRF recall without any error or warning.
--
-- Run twice: once on a cold cache (first run after idle), once after a few warm-up
-- queries to see whether warm cache resolves the delivery gap.
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT
    id,
    ROW_NUMBER() OVER (
        ORDER BY embedding <=> current_setting('myvars.embedding')::vector
    ) AS rank,
    1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 200;
-- Interpretation:
--   actual rows = 200 → HNSW delivered the full request; ef_search is sufficient.
--   actual rows < 200 → ef_search ceiling hit; the hybrid vector CTE gets fewer
--                        candidates than vector_limit = top_k * 2 intended.
--   If rows < 200 on cold cache but = 200 on warm cache → I/O bound, not ef_search bound.
--   If rows < 200 on warm cache too → raise hnsw.ef_search or reduce top_k multiplier.


-- ==============================================================================
-- 8. GRANULAR FILTERED SEARCH & PLANNER THRESHOLD SWEEP
-- ==============================================================================
-- Objective: Find the exact top_k where the planner switches from HNSW to
-- brute-force bitmap scan, and test whether compound filters (lower selectivity)
-- shift that tipping point to a lower LIMIT value.

-- 8.0. Session Variables for Filter Experiments
-- Set these values before running any EXPLAIN blocks in this section.
-- Adjust myvars.filter_book_chapter_chapter to a real chapter number from Section 6.2.

-- HIGH selectivity — largest book (~8.4% of rows). Hypothesis: tipping point at ~42.
SET myvars.filter_book_high           = '06. Lord of Chaos';

-- LOW selectivity — smallest book (~2.6% of rows). Hypothesis: tipping point at ~42.
SET myvars.filter_book_low            = '00. New Spring';

-- VERY LOW selectivity — single chapter within a small book (~0.01-0.1% of rows).
-- Hypothesis: compound filter causes planner to abandon HNSW at a much smaller LIMIT.
SET myvars.filter_compound_book       = '00. New Spring';
SET myvars.filter_compound_chapter    = '1';  -- replace with a real chapter number from 6.2


-- 8.1. Row-Count Confirmation for Each Filter Level
-- Always confirm actual selectivity before running EXPLAIN — it drives everything.
SELECT
    '00. New Spring (single book)'  AS filter_description,
    COUNT(*)                        AS matching_rows,
    ROUND(COUNT(*) * 100.0 / 2500000, 4) AS pct_of_total
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring'
UNION ALL
SELECT
    '06. Lord of Chaos (single book)',
    COUNT(*),
    ROUND(COUNT(*) * 100.0 / 2500000, 4)
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '06. Lord of Chaos';

-- Confirm compound filter selectivity (replace chapter value as needed).
SELECT
    COUNT(*)                             AS matching_rows,
    ROUND(COUNT(*) * 100.0 / 2500000, 6) AS pct_of_total
FROM wot_chunks_2_5m
WHERE metadata->>'book_name'            = current_setting('myvars.filter_compound_book')
  AND (metadata->>'chapter_number')::int = current_setting('myvars.filter_compound_chapter')::int;


-- ----------------------------------------------------------------------------
-- 8.2. LOW-Selectivity Single-Filter Sweep (book = '00. New Spring', ~2.6%)
-- Watch for: plan changes from "Index Scan using *_embedding_idx" (HNSW)
--            to "Bitmap Heap Scan" + "Sort" (brute-force).
-- Run each block separately and record the switch point.
-- ----------------------------------------------------------------------------

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 10;

-- HNSW

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 20;

-- HNSW

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 30;

-- HNSW

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 35;

-- HNSW

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 38;

-- "Bitmap Heap Scan" + "Sort" (brute-force) Execution Time: 41019.947 ms


-- ----------------------------------------------------------------------------
-- 8.3. HIGH-Selectivity Single-Filter Sweep (book = '06. Lord of Chaos', ~8.4%)
-- Hypothesis: more matching rows → brute-force appears cheaper sooner →
-- planner switches at the same or lower LIMIT than the low-selectivity case.
-- ----------------------------------------------------------------------------


EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_high')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 20;

-- HNSW


EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_high')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 30;

-- HNSW


EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_high')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 35;

-- HNSW

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_high')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 37;

-- HNSW


EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_high')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 38;

-- "Bitmap Heap Scan" + "Sort" (brute-force) Execution Time: 479739.632 ms


-- ----------------------------------------------------------------------------
-- 8.4. VERY LOW-Selectivity Compound Filter Sweep (book AND chapter, ~0.01-0.1%)
-- Hypothesis: the planner abandons HNSW at a much smaller LIMIT because each
-- discarded HNSW candidate requires the graph to explore ~1/selectivity nodes
-- before finding one match, making the HNSW path cost estimate rise sharply.
-- ----------------------------------------------------------------------------

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name'            = current_setting('myvars.filter_compound_book')
  AND (metadata->>'chapter_number')::int = current_setting('myvars.filter_compound_chapter')::int
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 5;

-- "Bitmap Heap Scan" + "Sort" (brute-force) Execution Time: 1920.700 ms

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name'            = current_setting('myvars.filter_compound_book')
  AND (metadata->>'chapter_number')::int = current_setting('myvars.filter_compound_chapter')::int
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 10;

-- "Bitmap Heap Scan" + "Sort" (brute-force) Execution Time: 1960.930 ms


EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name'            = current_setting('myvars.filter_compound_book')
  AND (metadata->>'chapter_number')::int = current_setting('myvars.filter_compound_chapter')::int
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 20;

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name'            = current_setting('myvars.filter_compound_book')
  AND (metadata->>'chapter_number')::int = current_setting('myvars.filter_compound_chapter')::int
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 50;


-- 8.5. Cost-Only Inspection (no actual execution — fast plan comparison)
-- Use EXPLAIN (COSTS) to read raw planner cost numbers without running the query.
-- Compare "total cost" of the HNSW path vs the Bitmap Heap Scan path at the boundary.
-- The plan that shows the LOWER total cost is what the planner will actually choose.


EXPLAIN (COSTS)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 35;

EXPLAIN (COSTS)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 40;

EXPLAIN (COSTS)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 42;

EXPLAIN (COSTS)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 50;


-- ==============================================================================
-- 9. HYBRID FILTERED SEARCH DIAGNOSTICS
-- ==============================================================================
-- Objective: Understand how adding a WHERE clause to the vector CTE or text CTE
-- inside a hybrid RRF query affects which indexes the planner chooses and how
-- long each component takes. Adds a metadata filter dimension to the Section 5
-- hybrid search baseline.

-- 9.1. Filtered Text CTE in Isolation
-- Does filtering the text CTE reduce the number of rows fetched from the heap?
-- Key question: can the GIN + B-tree combination prune rows before ts_rank scoring,
-- or does it still fetch all keyword-matching rows regardless of book_name?
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT
    id,
    ROW_NUMBER() OVER (
        ORDER BY ts_rank(
            to_tsvector('english', content),
            plainto_tsquery('english', current_setting('myvars.keyword'))
        ) DESC
    ) AS rank,
    ts_rank(
        to_tsvector('english', content),
        plainto_tsquery('english', current_setting('myvars.keyword'))
    ) AS text_score
FROM wot_chunks_2_5m
WHERE
    to_tsvector('english', content) @@ plainto_tsquery('english', current_setting('myvars.keyword'))
    AND metadata->>'book_name' = current_setting('myvars.filter_book_low')
LIMIT 100;


-- 9.2. Hybrid RRF: Filter on Text CTE Only (vector CTE stays pure HNSW)
-- Safe approach: the vector CTE has no WHERE clause so HNSW is always used.
-- The filter on the text CTE may reduce the heap rows scored by ts_rank.
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
WITH vector_search AS (
    SELECT
        id,
        ROW_NUMBER() OVER (ORDER BY embedding <=> current_setting('myvars.embedding')::vector) AS rank,
        1 - (embedding <=> current_setting('myvars.embedding')::vector) AS vector_similarity
    FROM wot_chunks_2_5m
    LIMIT 200
),
text_search AS (
    SELECT
        id,
        ROW_NUMBER() OVER (
            ORDER BY ts_rank(
                to_tsvector('english', content),
                plainto_tsquery('english', current_setting('myvars.keyword'))
            ) DESC
        ) AS rank,
        ts_rank(
            to_tsvector('english', content),
            plainto_tsquery('english', current_setting('myvars.keyword'))
        ) AS text_score
    FROM wot_chunks_2_5m
    WHERE
        to_tsvector('english', content) @@ plainto_tsquery('english', current_setting('myvars.keyword'))
        AND metadata->>'book_name' = current_setting('myvars.filter_book_low')
    LIMIT 200
),
rrf_fusion AS (
    SELECT
        COALESCE(v.id, t.id)                                           AS id,
        COALESCE(1.0 / (60 + v.rank), 0.0)
        + COALESCE(1.0 / (60 + t.rank), 0.0)                          AS rrf_score,
        v.vector_similarity,
        t.text_score
    FROM vector_search v
    FULL OUTER JOIN text_search t ON v.id = t.id
)
SELECT id, rrf_score, vector_similarity, text_score
FROM rrf_fusion
ORDER BY rrf_score DESC
LIMIT 50;


-- 9.3. Hybrid RRF: Filter Applied to BOTH CTEs
-- The vector CTE now has a WHERE clause → same HNSW tipping-point risk as Section 8.
-- Compare this plan against 9.2 to quantify the cost of filtering the vector CTE.
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
WITH vector_search AS (
    SELECT
        id,
        ROW_NUMBER() OVER (ORDER BY embedding <=> current_setting('myvars.embedding')::vector) AS rank,
        1 - (embedding <=> current_setting('myvars.embedding')::vector) AS vector_similarity
    FROM wot_chunks_2_5m
    WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')  -- filter on vector CTE
    LIMIT 200
),
text_search AS (
    SELECT
        id,
        ROW_NUMBER() OVER (
            ORDER BY ts_rank(
                to_tsvector('english', content),
                plainto_tsquery('english', current_setting('myvars.keyword'))
            ) DESC
        ) AS rank,
        ts_rank(
            to_tsvector('english', content),
            plainto_tsquery('english', current_setting('myvars.keyword'))
        ) AS text_score
    FROM wot_chunks_2_5m
    WHERE
        to_tsvector('english', content) @@ plainto_tsquery('english', current_setting('myvars.keyword'))
        AND metadata->>'book_name' = current_setting('myvars.filter_book_low')  -- filter on text CTE
    LIMIT 200
),
rrf_fusion AS (
    SELECT
        COALESCE(v.id, t.id)                                           AS id,
        COALESCE(1.0 / (60 + v.rank), 0.0)
        + COALESCE(1.0 / (60 + t.rank), 0.0)                          AS rrf_score,
        v.vector_similarity,
        t.text_score
    FROM vector_search v
    FULL OUTER JOIN text_search t ON v.id = t.id
)
SELECT id, rrf_score, vector_similarity, text_score
FROM rrf_fusion
ORDER BY rrf_score DESC
LIMIT 50;


-- 9.4. Hybrid RRF: Compound Filter on Both CTEs (book AND chapter)
-- Maximum selectivity restriction on a hybrid query. Tests HNSW abandonment
-- in the hybrid context with very few matching rows per CTE.
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
WITH vector_search AS (
    SELECT
        id,
        ROW_NUMBER() OVER (ORDER BY embedding <=> current_setting('myvars.embedding')::vector) AS rank,
        1 - (embedding <=> current_setting('myvars.embedding')::vector) AS vector_similarity
    FROM wot_chunks_2_5m
    WHERE metadata->>'book_name'            = current_setting('myvars.filter_compound_book')
      AND (metadata->>'chapter_number')::int = current_setting('myvars.filter_compound_chapter')::int
    LIMIT 200
),
text_search AS (
    SELECT
        id,
        ROW_NUMBER() OVER (
            ORDER BY ts_rank(
                to_tsvector('english', content),
                plainto_tsquery('english', current_setting('myvars.keyword'))
            ) DESC
        ) AS rank,
        ts_rank(
            to_tsvector('english', content),
            plainto_tsquery('english', current_setting('myvars.keyword'))
        ) AS text_score
    FROM wot_chunks_2_5m
    WHERE
        to_tsvector('english', content) @@ plainto_tsquery('english', current_setting('myvars.keyword'))
        AND metadata->>'book_name'            = current_setting('myvars.filter_compound_book')
        AND (metadata->>'chapter_number')::int = current_setting('myvars.filter_compound_chapter')::int
    LIMIT 200
),
rrf_fusion AS (
    SELECT
        COALESCE(v.id, t.id)                                           AS id,
        COALESCE(1.0 / (60 + v.rank), 0.0)
        + COALESCE(1.0 / (60 + t.rank), 0.0)                          AS rrf_score,
        v.vector_similarity,
        t.text_score
    FROM vector_search v
    FULL OUTER JOIN text_search t ON v.id = t.id
)
SELECT id, rrf_score, vector_similarity, text_score
FROM rrf_fusion
ORDER BY rrf_score DESC
LIMIT 50;


-- ==============================================================================
-- 10. WORKAROUND EVALUATION & DIAGNOSTICS
-- ==============================================================================
-- Each approach is tested with:
--   (a) EXPLAIN ANALYZE — to verify whether the planner selects HNSW
--   (b) Actual result query — to collect IDs for accuracy comparison (Section 10.8)
-- All settings are reset after each block to prevent cross-contamination.
-- Run against: WHERE book_name = '00. New Spring' LIMIT 50 (confirmed brute-force zone).

-- ------------------------------------------------------------------------------
-- 10.0. BASELINE — Default plan at the known tipping point
-- This is the problem state. Captures the brute-force plan and result IDs.
-- ------------------------------------------------------------------------------

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 50;

-- Baseline actual results (store these IDs mentally or in a temp table — see Section 10.8).
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 50;


-- ------------------------------------------------------------------------------
-- 10.1. WORKAROUND: enable_bitmapscan = off
-- Forces the planner away from Bitmap Heap Scan. It typically falls back to a
-- regular B-tree index scan + sort — still brute-force, but avoids the bitmap path.
-- Expected outcome: ~800 ms (warm cache). Plan should NOT show "Bitmap Heap Scan".
-- ------------------------------------------------------------------------------

SET enable_bitmapscan = off;

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 50;

SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 50;

RESET enable_bitmapscan;


-- ------------------------------------------------------------------------------
-- 10.2. WORKAROUND: Disable all non-HNSW scan types
-- Removes every alternative except the vector index. PostgreSQL treats enable_*
-- settings as strong preferences, not hard constraints — it may still use a
-- B-tree index scan if it calculates no viable alternative.
-- Expected outcome: ~818 ms. HNSW rarely chosen — planner overrides the hints.
-- ------------------------------------------------------------------------------

SET enable_seqscan    = off;
SET enable_bitmapscan = off;
SET enable_indexscan  = off;

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 50;

SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 50;

RESET enable_seqscan;
RESET enable_bitmapscan;
RESET enable_indexscan;


-- ------------------------------------------------------------------------------
-- 10.3. WORKAROUND: hnsw.iterative_scan = relaxed_order
-- Gives the HNSW index the mechanical ability to resume scanning in batches
-- past the initial ef_search window. Solves the index-level limitation only.
-- Expected outcome: ~795 ms. Planner STILL chooses bitmap scan — iterative scan
-- never activates because it requires the planner to select HNSW first.
-- ------------------------------------------------------------------------------

SET hnsw.iterative_scan = relaxed_order;

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 50;

SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 50;

RESET hnsw.iterative_scan;


-- ------------------------------------------------------------------------------
-- 10.4. WORKAROUND: iterative_scan + disable all other scan types
-- Combines both approaches from 10.2 and 10.3. Even with no alternatives,
-- the planner may still choose a B-tree index scan over the HNSW index.
-- Expected outcome: ~871 ms. Still no HNSW usage.
-- ------------------------------------------------------------------------------

SET hnsw.iterative_scan = relaxed_order;
SET enable_seqscan      = off;
SET enable_bitmapscan   = off;
SET enable_indexscan    = off;

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 50;

SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 50;

RESET hnsw.iterative_scan;
RESET enable_seqscan;
RESET enable_bitmapscan;
RESET enable_indexscan;


-- ------------------------------------------------------------------------------
-- 10.5. WORKAROUND: iterative_scan strict_order + large ef_search
-- Strict ordering guarantees results in exact distance order; large ef_search
-- gives the HNSW graph a wide enough candidate pool to find 50 filtered results.
-- Still requires the planner to select HNSW — which it typically will not at LIMIT 50.
-- Expected outcome: ~795 ms (same as 10.3). The ef_search change is irrelevant
-- if the planner never enters the HNSW index.
-- ------------------------------------------------------------------------------

SET hnsw.iterative_scan = strict_order;
SET hnsw.ef_search      = 400;

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 50;

SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 50;

RESET hnsw.iterative_scan;
RESET hnsw.ef_search;


-- ------------------------------------------------------------------------------
-- 10.6. WORKAROUND (BEST): CTE Over-Fetch Pattern
-- The inner CTE performs a pure unfiltered vector scan — no WHERE clause means
-- the planner always chooses HNSW. The metadata filter is applied AFTER the HNSW
-- results are materialized. Over-fetch = ceil(top_k / filter_selectivity).
-- For 2.6% selectivity and top_k=50: ceil(50 / 0.026) ≈ 1,924 → use 2,000.
-- Expected outcome: ~187 ms. First workaround to actually use HNSW.
-- ------------------------------------------------------------------------------

-- 10.6a. Plan inspection (over-fetch 2000 candidates for top_k=50 at 2.6% selectivity).
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
WITH nearest AS (
    SELECT id,
           1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity,
           metadata
    FROM wot_chunks_2_5m
    ORDER BY embedding <=> current_setting('myvars.embedding')::vector
    LIMIT 2000  -- over-fetch: 50 * ceil(1 / 0.026)
)
SELECT id, similarity
FROM nearest
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY similarity DESC
LIMIT 50;

-- 10.6b. Actual results for accuracy comparison (see Section 10.8).
WITH nearest AS (
    SELECT id,
           1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity,
           metadata
    FROM wot_chunks_2_5m
    ORDER BY embedding <=> current_setting('myvars.embedding')::vector
    LIMIT 2000
)
SELECT id, similarity
FROM nearest
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY similarity DESC
LIMIT 50;

-- 10.6c. Sensitivity: what happens if the over-fetch is halved (1000 instead of 2000)?
-- Expected: returns fewer than 50 rows when over-fetch is too low for the selectivity.
WITH nearest AS (
    SELECT id,
           1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity,
           metadata
    FROM wot_chunks_2_5m
    ORDER BY embedding <=> current_setting('myvars.embedding')::vector
    LIMIT 1000  -- 50 * 20 — may not yield 50 results at 2.6% selectivity
)
SELECT id, similarity
FROM nearest
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY similarity DESC
LIMIT 50;

-- 10.6d. CTE over-fetch with compound filter (very low selectivity).
-- For a chapter representing ~0.1% of data, over-fetch = ceil(50 / 0.001) = 50,000.
-- At that scale the over-fetch itself becomes a bottleneck — demonstrates the
-- practical limit of this workaround for highly selective compound filters.
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
WITH nearest AS (
    SELECT id,
           1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity,
           metadata
    FROM wot_chunks_2_5m
    ORDER BY embedding <=> current_setting('myvars.embedding')::vector
    LIMIT 5000  -- adjust based on actual chapter selectivity from Section 8.1
)
SELECT id, similarity
FROM nearest
WHERE metadata->>'book_name'            = current_setting('myvars.filter_compound_book')
  AND (metadata->>'chapter_number')::int = current_setting('myvars.filter_compound_chapter')::int
ORDER BY similarity DESC
LIMIT 50;


-- ------------------------------------------------------------------------------
-- 10.7. WORKAROUND: Raise cpu_operator_cost (session-level planner tuning)
-- Increases the planner's per-operator cost so vector distance is no longer
-- priced like a scalar comparison. Shifts the HNSW vs brute-force break-even
-- point higher, keeping HNSW active at larger LIMIT values.
-- Default: 0.0025. Try: 0.05 (20x), 0.1 (40x), 0.5 (200x).
-- This is a session-only change — reset it after testing.
-- ------------------------------------------------------------------------------

SET cpu_operator_cost = 0.1;  -- 40x the default

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 50;

-- Run at higher top_k to see how far the break-even point shifts.
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 100;

RESET cpu_operator_cost;

-- Try a more conservative multiplier to find the minimum value that keeps HNSW active.
SET cpu_operator_cost = 0.05;  -- 20x

EXPLAIN (COSTS)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 50;

RESET cpu_operator_cost;


-- ------------------------------------------------------------------------------
-- 10.8. WORKAROUND: Hybrid Filtered Search — CTE Over-Fetch Pattern
-- Applies the over-fetch technique to the vector CTE inside a hybrid RRF query.
-- The text CTE can safely carry the WHERE clause (GIN index, no HNSW to abandon).
-- Vector candidates are over-fetched unfiltered, then filtered post-materialization.
-- ------------------------------------------------------------------------------

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
WITH vector_candidates AS (
    -- Pure unfiltered HNSW scan — always uses the vector index.
    SELECT
        id,
        1 - (embedding <=> current_setting('myvars.embedding')::vector) AS vector_similarity,
        metadata
    FROM wot_chunks_2_5m
    ORDER BY embedding <=> current_setting('myvars.embedding')::vector
    LIMIT 2000  -- over-fetch; filter applied in the next CTE
),
filtered_vector AS (
    -- Apply metadata filter after HNSW materialization.
    SELECT
        id,
        ROW_NUMBER() OVER (ORDER BY vector_similarity DESC) AS rank,
        vector_similarity
    FROM vector_candidates
    WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
    LIMIT 100
),
text_search AS (
    -- Full-text search with filter — safe because GIN is not subject to the same planner flaw.
    SELECT
        id,
        ROW_NUMBER() OVER (
            ORDER BY ts_rank(
                to_tsvector('english', content),
                plainto_tsquery('english', current_setting('myvars.keyword'))
            ) DESC
        ) AS rank,
        ts_rank(
            to_tsvector('english', content),
            plainto_tsquery('english', current_setting('myvars.keyword'))
        ) AS text_score
    FROM wot_chunks_2_5m
    WHERE
        to_tsvector('english', content) @@ plainto_tsquery('english', current_setting('myvars.keyword'))
        AND metadata->>'book_name' = current_setting('myvars.filter_book_low')
    LIMIT 100
),
rrf_fusion AS (
    SELECT
        COALESCE(v.id, t.id)                                           AS id,
        COALESCE(1.0 / (60 + v.rank), 0.0)
        + COALESCE(1.0 / (60 + t.rank), 0.0)                          AS rrf_score,
        v.vector_similarity,
        t.text_score
    FROM filtered_vector v
    FULL OUTER JOIN text_search t ON v.id = t.id
)
SELECT id, rrf_score, vector_similarity, text_score
FROM rrf_fusion
ORDER BY rrf_score DESC
LIMIT 50;


-- ==============================================================================
-- 11. ACCURACY & RECALL MEASUREMENT
-- ==============================================================================
-- Compares the result sets produced by each workaround against a ground-truth
-- sequential scan. The ground truth is computed by forcing a full seq scan
-- (disable all indexes), which guarantees exact nearest-neighbor ordering.
-- Recall = overlap_count / ground_truth_count * 100.

-- 11.1. Compute ground-truth result set (exact brute-force, no index).
-- WARNING: On 2.5M rows at 1536 dimensions this will be slow (~215 s).
-- Run once, store in a temp table, reuse for all comparisons.

DROP TABLE IF EXISTS gt_results;
CREATE TEMP TABLE gt_results AS
SELECT
    id,
    1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity,
    ROW_NUMBER() OVER (
        ORDER BY embedding <=> current_setting('myvars.embedding')::vector
    ) AS gt_rank
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 50;
-- Note: because of the WHERE clause and no HNSW at LIMIT=50, this IS the brute-force plan.
-- Verify with EXPLAIN before treating it as ground truth.


-- 11.2. Compute CTE over-fetch result set (Workaround 10.6).
DROP TABLE IF EXISTS cte_results;
CREATE TEMP TABLE cte_results AS
WITH nearest AS (
    SELECT id,
           1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity,
           metadata
    FROM wot_chunks_2_5m
    ORDER BY embedding <=> current_setting('myvars.embedding')::vector
    LIMIT 2000
)
SELECT
    id,
    similarity,
    ROW_NUMBER() OVER (ORDER BY similarity DESC) AS cte_rank
FROM nearest
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY similarity DESC
LIMIT 50;


-- 11.3. Recall: How many IDs from the over-fetch workaround appear in ground truth?
SELECT
    (SELECT COUNT(*) FROM gt_results)                                        AS gt_count,
    (SELECT COUNT(*) FROM cte_results)                                       AS cte_count,
    COUNT(*)                                                                 AS overlap_count,
    ROUND(COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM gt_results), 0), 2) AS recall_pct
FROM gt_results g
JOIN cte_results c ON g.id = c.id;


-- 11.4. Rank displacement: for each overlapping ID, how far is its rank in the
-- CTE result from its rank in the ground truth? High displacement means the
-- workaround returns the right IDs but in a different order.
SELECT
    g.id,
    g.gt_rank,
    c.cte_rank,
    ABS(g.gt_rank - c.cte_rank) AS rank_displacement,
    g.similarity                AS gt_similarity,
    c.similarity                AS cte_similarity,
    ABS(g.similarity - c.similarity) AS similarity_delta
FROM gt_results g
JOIN cte_results c ON g.id = c.id
ORDER BY g.gt_rank;


-- 11.5. Missing results: IDs in ground truth but NOT returned by the CTE workaround.
-- Any row here is a recall miss — the over-fetch multiplier was too small, or the
-- HNSW index did not explore that region of the graph.
SELECT g.id, g.gt_rank, g.similarity AS gt_similarity
FROM gt_results g
LEFT JOIN cte_results c ON g.id = c.id
WHERE c.id IS NULL
ORDER BY g.gt_rank;


-- 11.6. False positives: IDs returned by CTE workaround but absent from ground truth.
-- These rows exist because HNSW is approximate — it may return slightly different
-- neighbors than an exact brute-force scan. Expected for ANN indexes.
SELECT c.id, c.cte_rank, c.similarity AS cte_similarity
FROM cte_results c
LEFT JOIN gt_results g ON c.id = g.id
WHERE g.id IS NULL
ORDER BY c.cte_rank;
