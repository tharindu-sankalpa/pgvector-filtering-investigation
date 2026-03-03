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
    AND metadata->>'book_name' = '06. Lord of Chaos'
LIMIT 10;

-- ------------------------------------------------------------------------------
-- 2. QUERY CONTEXT SETUP
-- ------------------------------------------------------------------------------

-- Set a session variable with the sample embedding extracted above.
-- This allows us to reuse the same vector in multiple queries without
-- pasting the huge array every time.
SET
myvars.embedding = '[-0.012095246,-0.029539807,-0.0048980475,-0.0057412894,-0.008557584,0.0031390984,-0.017892532,-0.06023907,-0.0010515816,-0.023716168,0.04442829,0.015942536,0.0043545524,-0.010454878,-0.01247734,0.010896262,0.029882373,0.035231687,0.021305025,-0.006307842,-0.0057083503,0.025310423,0.0048980475,0.009031908,-0.0021097509,-0.012378523,0.003534368,-0.025073262,0.0021953927,-0.023281373,-0.0028047664,-0.005751171,-0.016561791,-0.036733713,-0.05412557,-0.009071435,-0.018076992,-0.044928964,0.0057709347,-0.018762125,-0.0065845307,0.0034091994,-0.0093217725,0.0010021728,-0.016192874,-0.0054283678,-0.016100643,0.005303199,-0.014137472,0.013636798,-0.0013669736,0.008063498,-0.031068182,0.023162792,-0.023215495,0.0013150945,0.0014822605,0.021792524,0.0122928815,-0.0068118107,-0.0093876505,-0.011192715,-0.009558934,-0.018011114,-0.009295421,-0.025692517,-0.020409081,-0.017273277,-0.022135092,-0.012668387,0.020843878,0.0046608862,-0.0067854594,-0.016548617,0.031674262,-0.0027800621,0.010962141,-0.0069040405,-0.005919161,0.0023255022,0.004917811,-0.0385256,-0.014111121,0.019513138,0.008781571,-0.011884436,0.005408604,0.011923963,-0.029223591,-0.017721249,0.013281055,0.01674625,0.0018100048,0.007332249,0.004334789,0.026601637,0.008577348,0.005896103,0.0055370666,-0.013004366,-0.010389,0.020988809,-0.0053229625,-0.012609096,-0.0101057235,-0.0055370666,-0.010527344,0.0008761807,0.028011432,0.002934876,-0.02898643,0.025758395,-0.00097499805,-0.02567934,0.001958231,-0.024216844,0.014427336,0.0018182397,-0.013057069,-0.020646242,-0.0030238116,0.011732916,0.0054678945,0.00054431905,0.008998969,0.003995516,-0.0035244862,0.0010186424,-0.0077933967,0.0029052307,0.024849275,0.016087469,0.002328796,-0.023689818,0.02059354,0.031516153,0.012668387,0.008847449,-0.0043084375,0.0030732204,-0.0015860188,0.029460752,-0.016087469,-0.013649973,0.006202437,-0.0043446706,0.010283595,-0.0031456864,-0.0066866423,-0.009769744,0.015468213,-0.023913804,0.015283753,-0.029829672,0.013649973,-0.010092547,0.016100643,-0.0070621483,-0.013090008,-0.011311295,0.0032790897,0.016667197,0.012997778,-0.011304707,0.010290182,0.018775301,0.00094617635,-0.013215177,0.01099508,0.003939519,0.008557584,0.019434083,-0.027299946,-0.0018149457,-0.0071741412,0.016469562,0.017273277,0.010204541,0.012286293,0.0028113544,-0.04037019,0.00049161643,0.015850307,0.018814828,-0.027774269,-0.00881451,-0.00085229985,0.0038242324,0.02798508,4.1456977e-05,-0.007958093,0.023544885,-0.0096314,-0.011403524,-0.6581501,-0.016126996,0.031753317,0.009203191,0.017918883,-0.008241368,-0.0024160848,-0.011976666,0.0022448013,0.019210096,-0.0054118983,0.009578697,0.00026721865,-4.7324258e-05,-0.018445909,-0.0032296812,-0.004120684,0.0016856596,0.0076418766,0.0042853802,-0.015705375,0.019144218,-0.00012084541,0.012622273,0.013274467,-0.0057083503,-0.014611796,0.0018017701,-0.043058023,0.018314153,-0.04018573,0.021608066,-0.012622273,0.027774269,0.050383683,-0.0015127293,-0.024111439,0.0114430515,-0.014519566,0.019210096,-0.029882373,-0.0037056515,0.006627352,-0.024809748,0.0024704344,0.0030024012,-0.00684475,-0.011772443,-0.013017542,0.021963809,0.019710772,-0.00081894896,-0.008564172,0.03080467,-0.0006233729,0.020343203,0.013030717,0.01090285,-0.0046872376,0.01635098,0.017167872,0.035442498,-0.0018017701,-0.03913168,-0.0135972705,0.0067525203,-0.010553695,0.02311009,0.016456386,-0.01081062,0.017391857,0.02625907,0.001803417,0.020659419,0.019420907,0.003153921,0.017549966,0.019763475,0.024058737,0.015402335,-0.0015959005,-0.033966824,-0.023083739,-0.014585444,0.013636798,-0.026812447,-0.014308755,0.0057050562,-0.02292563,0.02516549,-0.008050322,0.031885073,0.007931741,-0.005490952,0.008702517,0.033413447,0.004634535,0.011555045,0.04179316,-0.014427336,0.01156822,-0.026786096,0.017997937,-0.00084200635,0.0070094457,0.008215018,-0.032543853,0.022635765,0.033308044,0.011614335,-0.011146599,0.0013356815,0.0001752979,-0.012207239,0.00013875606,-0.017813478,0.013518216,0.032728314,0.00033556734,-0.030567506,0.010013494,0.008643226,0.006212319,-0.0050462736,0.021568539,0.0238084,-0.0018676483,0.028037783,-0.01129812,-0.00040844514,-0.00271089,0.0034059053,0.022938807,-0.0023847925,0.017484087,-0.0077802213,0.015072943,-0.022688469,0.04345329,-0.00851147,-0.0047597033,0.009987143,0.008399476,-0.0072729588,0.009302009,-0.019750299,-0.03607493,0.002259624,-0.020250974,0.0033663784,0.008135963,0.0068315743,-0.023386778,0.016166521,0.01816922,0.009163665,-0.014018891,0.0030353402,-0.01793206,0.0006344899,0.014980714,0.0036265976,0.0003825056,-0.02329455,-0.020132393,-0.028116837,0.0035574255,-0.0004953221,0.0018939996,-0.042346537,0.0016790718,-0.0304094,0.0021196327,0.011877848,-0.011673626,-0.0040712757,-0.012082071,0.011416701,0.017681722,-0.0032823838,0.01886753,-0.01886753,-0.008702517,0.004825582,0.045350585,0.00041997383,-0.0030320464,-0.013531392,-0.008906739,-0.020554014,-0.03346615,-0.0021146918,0.017997937,0.004476427,0.033176284,0.007786809,0.008689341,0.006759108,-0.0036265976,0.024717519,0.005224145,0.0015876658,0.034230337,-0.005793992,0.03206953,-0.022240497,0.005566712,-0.03686547,0.030093184,-0.012997778,0.0034454323,-0.019434083,0.008834273,-0.0016543675,-0.025534408,0.033308044,-0.0035146046,0.006660291,-0.00045991252,0.020224622,0.00054761296,-0.023637116,0.021950632,-0.0035442498,0.0022464483,0.0029562863,0.027510757,0.006689936,0.010612986,0.000506439,-0.0039263438,0.014532741,-0.015020241,0.005072625,-0.015850307,-0.009440353,0.018485436,-0.017101994,0.013063656,0.0031835663,-0.0113771735,0.005316375,0.02313644,-0.017681722,-0.009591873,-0.013900311,0.026720218,-0.010863323,0.005072625,0.009664339,0.014242877,-0.0029167593,-0.009605048,0.0064560683,-0.007371776,-0.015942536,0.017194223,0.0020010518,0.012075483,0.043137077,0.006512065,0.007101675,0.020830702,-0.003455314,0.01996111,0.0019285857,0.017589493,0.01562632,-0.008577348,-0.0008794746,-0.0072004925,-0.006462656,0.0046444163,0.016416859,0.03752425,0.0043743155,-0.002934876,-0.0035837768,0.012905549,-0.005079213,-0.0056260023,-0.021067863,0.015520915,0.015982063,-0.0021163388,-0.006077268,-0.036417495,0.009302009,-0.00021575126,-0.00014071183,-0.012483928,0.00790539,-0.01744456,-0.0009939381,-0.007714343,0.0014995537,0.037181683,-0.018419558,0.0011141659,0.0026598342,-0.0009222955,-0.020791175,-0.010975316,-0.008774983,0.021436783,0.008643226,0.010395587,-0.020369554,0.0027405352,-0.009848798,-0.010869911,0.006077268,0.018854355,-0.02341313,0.017036116,-0.00024971974,-0.018116519,0.01023748,0.05230733,-0.005985039,0.014822606,-0.03488912,0.0055601243,0.0020965752,0.101347096,0.012121597,0.0039592828,-0.0021245736,-0.008557584,-0.005415192,-0.011910787,-0.013847608,-0.003265914,0.009585286,0.010718391,0.004743234,0.0077999844,-0.009078023,0.024717519,-0.0036035401,-0.004555481,-0.027906027,-0.009710453,-0.023255022,-0.013393048,0.026496232,0.0026532465,0.0052307327,-0.016943885,-0.009605048,-0.0015481388,0.0006855455,0.010784269,-0.0006661938,0.012826495,0.017642194,-0.004315025,0.016482737,-0.010402176,-0.006624058,0.0101057235,0.013300818,0.027089136,-0.04163505,0.011825145,0.0041832686,0.025771571,-0.013531392,0.017141521,-0.0023238552,0.0036035401,0.018287802,-0.0054250737,-0.013768554,0.0568134,0.0016197815,-0.022965157,-0.011950314,0.028485755,0.031674262,-0.0233209,-0.00085477025,-0.010250655,0.06767014,-0.0043413765,0.002743829,-0.02353171,-0.011943727,-0.018511787,-0.016258752,-0.0064923014,-0.0033317923,-0.013821256,-0.0002822471,-0.0010952259,-0.0006546651,-0.005645766,0.0029052307,0.018406382,0.023544885,0.000686369,-0.0020339908,-0.023031035,0.014690849,-0.0033350864,-0.018999286,-0.0002972756,-0.014993889,0.0065384163,0.015257402,-0.007108263,4.022176e-05,-0.0077011674,0.0038670532,0.03665466,-0.002589015,0.017945236,-0.011983253,0.0066767605,0.0029134655,0.010000318,0.0029875785,-0.012398287,0.0032016828,0.033334393,-0.054230973,0.0038077626,-0.013096596,-0.004473133,-0.0035475437,0.0018544727,0.030198589,-0.0021706882,-0.02861751,-0.0062287883,-0.0077275187,0.011462815,-0.008577348,0.0015596675,0.025692517,0.01683848,0.028248593,0.016601318,0.016654022,0.01595571,-0.03976411,0.03122629,-0.006169498,-0.025389478,0.015257402,0.0065944125,-0.016087469,-0.019605367,0.01968442,0.0008794746,0.017181046,-0.002969462,0.011620923,-0.03204318,-0.0067854594,-0.0049935714,-0.024414478,-0.026509408,0.0052768476,0.004486309,-0.009308596,-0.017971586,-0.022754347,-0.0099080885,-0.020132393,0.009493056,5.1132844e-05,-0.014717201,0.031331696,-0.013109772,-0.0009906442,-0.01571855,0.009486468,-0.012648623,-0.026061434,-0.012312644,-0.013715851,0.026245894,0.01765537,0.019486785,0.0023814987,0.0034059053,0.013900311,-0.0144141605,-0.0047498215,-0.0011479285,-0.0084060645,-0.041476946,0.03388777,-0.012872609,0.019842528,0.008959441,-0.015481388,2.6582902e-05,0.022029687,-0.023558062,-0.007628701,-0.016601318,-0.036681008,-0.015494564,-0.0135972705,0.0014567327,0.019355029,-0.007931741,-0.013274467,0.019315502,-0.008221606,-0.00025177843,-0.020962458,0.032833718,-0.008241368,-0.010033257,-0.018274626,-0.015995238,-0.016996589,0.007378364,0.010323121,-0.00896603,-0.0074244784,0.008116201,0.005563418,0.0053690774,0.0033202637,-0.016285103,0.010230892,-0.008300659,-0.013070244,-0.004450076,-0.029460752,0.0047662915,-0.023874277,-0.022451308,-0.001656838,-0.007944916,0.0051286216,-0.031331696,0.019170571,-0.01686483,-0.023175968,0.020395905,-0.020106042,0.032728314,-0.0074376543,0.029302645,0.0129450755,0.006759108,-0.00081771374,0.015758077,-0.025969205,-0.012338996,0.012655212,-0.0032840306,-0.009624812,-0.02453306,0.0054448373,0.027879674,-0.029144537,-0.020092865,0.023782048,0.01744456,-0.007958093,-0.014993889,-0.0017029527,-0.003606834,0.005836813,-0.036522903,-0.0040086913,0.0075825863,0.008023971,-0.015889833,0.0024984325,0.006917216,0.029750617,-0.015995238,-0.010784269,-0.014242877,0.004371022,-0.0009881738,0.021054689,-0.054072864,0.01511247,-0.01571855,-0.021186445,0.0039230497,-0.0016733075,-0.0167199,0.0016280162,-0.011535281,0.02241178,0.0005509069,-0.012154536,0.014717201,0.007002858,0.01590301,-0.012483928,-0.015455037,-0.0035211924,-0.0064033656,-0.007977855,0.0070160334,-0.015520915,-0.017431384,-0.010883086,-0.021357728,-0.0026021907,-0.005754465,-0.0017869475,0.018735774,-0.037260737,-0.020698946,0.0010664042,-0.0015423745,0.017299628,-0.004930987,-0.016074292,-0.0042952616,0.025020558,-0.013267879,0.018314153,-0.024454005,0.0073520127,-0.0019071753,-0.020422257,0.003540956,-0.008247957,0.0093876505,2.997975e-05,0.008702517,0.014097945,0.009143901,-0.005257084,-0.020409081,-0.02901278,0.010830384,0.0035442498,0.009822447,-0.019499961,-0.025191842,0.004025161,-0.0030896899,0.003916462,0.021581715,-0.033097234,-0.006129971,0.021568539,0.013979364,0.010797445,-0.017918883,0.011126836,0.0056227087,0.013570919,-0.020738473,-0.037260737,0.007925154,-0.024638465,-0.009229543,0.017589493,0.013702676,0.0021229265,0.0050265105,0.017391857,0.017378682,0.008979205,-0.01272109,0.0027273595,0.0016222518,0.0015110823,-0.018801652,-0.021252323,-0.017563142,0.024454005,-0.002068577,-0.014677674,-0.007674816,0.017115168,-0.03164791,0.0036298914,-0.031120885,0.00549754,0.019131044,0.0018281214,-0.0060640927,0.023518534,0.0041602114,-0.019710772,0.013887134,-0.008103024,-0.014071594,-0.019249624,0.01150893,0.032122232,-0.010158426,0.005451425,-0.0033400273,0.0025264309,-0.020883406,-0.014045242,-0.015349632,0.006508771,0.0021525717,-0.00549754,0.0065219468,-0.003797881,0.0020224622,-0.018524963,0.010863323,0.013129535,0.00094864675,-0.032675613,-0.024190493,0.026917852,0.016640846,0.006462656,-0.036575604,-0.012134774,-0.0023518535,-0.010395587,0.03628574,-0.00031930362,-0.051226925,0.0345202,-0.0020768119,-0.016957061,-0.025007384,0.019803,-0.0384729,0.012062307,0.00993444,-0.027062785,0.018972935,0.0014789667,-0.005336138,-0.03064656,0.0037912931,0.0074178907,0.002248095,-0.037392493,0.026944203,0.021924281,-0.014242877,0.018735774,-0.037471548,-0.017813478,-0.005790698,-0.032781016,0.0024309075,-0.027010081,-0.0284067,0.0019714066,0.003534368,-0.020778,0.0042326776,0.0092558935,-0.022003334,0.028301295,0.23252386,-0.010336298,-0.033782367,0.01635098,-0.0035277803,0.0019747005,0.0304094,-0.0022727996,0.00084200635,-0.01450639,-0.016179698,-0.00917684,-0.028749267,-0.009394238,0.0076155253,-0.012338996,-0.024704343,-0.013136122,-0.0069040405,-0.028433053,0.018024288,0.012134774,-0.022688469,-0.0071280263,0.032675613,0.011436464,-0.010118899,0.02504691,0.012009605,0.0062979604,-0.0025725455,0.028327648,0.013109772,-0.0064132474,-0.01581078,0.005576594,0.025916504,0.02087023,-0.0032214464,0.024822924,-0.012352171,0.006479126,-0.002480316,-0.004137154,-0.004120684,0.035363443,-0.021976983,-0.011686801,-0.006591119,0.012464165,-0.009473292,-0.0048684026,0.01338646,0.035995875,-0.009427178,0.023953332,0.027458053,-0.0021311613,0.0020026988,0.028564809,-0.03831479,0.0073915394,0.0032181523,0.01798476,-0.019499961,0.027906027,0.00020309028,-0.008287484,-0.01532328,-0.002111398,0.012378523,-0.004812406,-0.0048552267,0.04037019,-0.034862768,-0.02071212,0.020211447,0.0027652394,-0.01044829,0.011620923,-0.018788476,0.00021801583,0.012918725,-0.008228193,-0.022609415,-0.022056038,0.008241368,0.008748631,-0.002068577,0.02513914,0.007569411,-0.013821256,0.0010581694,-0.013267879,0.051753953,0.0065746494,-0.004476427,0.012549806,-0.018024288,0.0014715553,-0.01835368,-0.050225575,0.0046114773,-0.00016057823,-0.007740694,0.018801652,-0.00023077974,0.0136895,-0.010606398,-0.0042195017,-0.012661799,-0.027642513,0.013584095,-0.0049046357,-0.009525995,0.0012870963,-0.0021492778,0.0045950077,-0.013412811,-0.005112152,-0.02038273,-0.01798476,0.0060509173,0.008208429,0.008544409,-0.005678705,-0.030462101,0.00054143684,0.0050199227,-0.024309073,-0.004450076,-0.011225654,0.0011676919,0.0013183884,0.006568061,0.014835781,0.013063656,0.012635448,-0.023202319,0.005066037,0.017642194,0.012213827,0.012437813,0.012740853,0.019513138,-0.022306375,0.014242877,-0.01262886,-0.014058419,-0.011693389,-0.052781653,-0.006884277,-0.0061398526,-0.012530043,0.032781016,-0.011844909,-0.03225399,-0.013050481,0.012859434,0.04558775,-0.026865149,-8.5590254e-05,0.030066833,-0.016548617,0.0020784587,0.008702517,-0.16780508,0.015863482,0.0023304431,-0.022517186,0.01399254,0.0047662915,0.016166521,-0.011238829,-0.034915473,0.0046147713,-0.00012413932,-0.0012031015,-0.028064134,0.012872609,0.0057281135,0.004061394,0.0059553934,0.03022494,0.012589334,0.031542506,0.016522264,-0.023096913,-0.0016428388,-0.012444401,0.00563259,-0.012108422,0.0008687694,0.032886423,0.008946266,-0.016443212,-0.034915473,0.0085048815,0.015982063,0.012378523,0.01108731,0.011034607,-0.01581078,0.01511247,0.013544568,0.024664816,0.012609096,0.02859116,-0.0064494805,-0.008129376,0.006054211,0.002822883,0.010474642,0.0015473154,0.011871261,-0.0070160334,-0.012852847,-0.015217875,0.010211129,-0.01166045,0.038999923,0.022306375,0.012833083,0.0026054848,0.0056622354,-0.010533932,0.028222242,-0.018643545,0.008794746,-0.0054448373,0.0014361459,0.015033416,-0.013307407,-0.018907057,-0.017220573,0.0044105486,-0.03531074,-0.017721249,0.01602159,-0.019499961,0.010316534,-0.012253354,0.00760235,-0.009749981,-0.00790539,-0.014572268,-0.026851974,0.042662755,-0.02019827,-0.013103183,0.024783397,0.016219225,0.008162315,0.01595571,-0.0058203433,-0.008946266,0.028143188,-0.02059354,-0.020237798,-0.028353998,0.018762125,0.029065482,0.0031143941,-0.024664816,-0.0068315743,-0.014427336,0.0010606397,-0.0049441624,0.00015790193,-0.00038394667,0.021673944,-0.00255937,-0.0075364714,0.00420962,0.020857053,-0.012273118,-0.018156046,-0.0037089454,0.0024951387,0.009987143,0.013353521,0.026219543,-0.0006431364,-0.032939125,-0.0008247133,0.009578697,0.06440257,0.024651641,-0.0009848798,-0.0059817447,-0.017997937,-0.025033735,-0.11910787,-0.01459862,-0.0075628227,0.0114101125,-0.008656402,-0.008425828,-0.007747282,-0.0065944125,0.0034124933,0.015494564,-0.00063613686,-0.02607461,-0.008070085,0.0077999844,0.020066515,-0.0053657833,0.00699627,-0.01438781,-0.026601637,0.037656005,-0.018340504,0.004371022,0.01105437,-0.05454719,-0.008972618,0.011910787,-0.020619892,0.030883722,0.021041512,0.021608066,-0.018814828,-0.014743552,0.033808716,-0.0108567355,-0.009374475,-0.013155886,-0.03346615,-0.014256053,0.015402335,-0.008682753,0.01108731,-0.0026532465,-0.0008498294,-0.02071212,-0.023044212,0.025244545,-0.019460434,0.02362394,-0.0041964445,0.0033383803,-0.017167872,0.0014402632,-0.027194541,0.007938329,0.037049927,0.00714779,0.013432574,0.0074705933,0.00065960595,0.015257402,0.01541551,0.017036116,-0.016219225,0.010461465,-0.019618543,-0.011746092,0.0023518535,0.0003160097,0.013781729,0.034441147,0.006271609,0.011047782,0.0035146046,0.0233209,-0.0026614813,0.020250974,-0.0051022703,-0.02056719,0.008017383,-0.028512105,-0.013353521,-0.01084356,0.018393207,-0.005701762,0.024032384,0.005448131,0.0067096995,-0.0039889277,0.013939837,0.015850307,-0.016311454,0.01865672,0.017760776,-0.039026275,-0.012819907,0.03122629,-0.010678864,-0.023465833,0.0021624535,0.0010038198,-0.02056719,-0.008188666,-0.025481706,0.03483642,-0.0059488057,-0.008491706,-0.008485118,-0.028274944,0.001558844,0.014585444,-0.004782761,-0.0004450899,-0.021713471,0.038631003,0.008886975,0.008919915,-0.03083102,-0.026100961,0.005306493,-0.018011114,0.019789826,0.010046433,-0.019763475,-0.009512819,-0.0011158128,0.018208748,0.010889675,-0.0069765067,-0.02674657,-0.0005509069,-0.013557743,0.004407255,0.026021907,-0.033703312,0.00993444,0.0027652394,-0.010481229,0.002145984,0.020909756,0.013755378,0.013425987,0.004394079,-0.040660053,-0.0065252404,0.008399476,-0.013458926,-0.0023897334,0.015639497,-0.003873641,0.011686801,0.017325979,-0.00083665375,0.00012537454,0.015375983,0.021186445,-0.012648623,-0.012635448,-0.018406382,0.01998746,-0.015428686,0.0026515995,-0.011093897,0.032306693,0.0059488057,-0.024480358,-0.019750299,0.024585763,-0.003348262,0.0070423847,0.014862133,0.00044303123,-0.03694452,-0.006202437,0.010942377,0.00563259,0.0058071674,0.0114101125,-0.0007876568,-0.011910787,0.007101675,0.0064857136,0.027642513,-0.025639813,0.01090285,-0.034599256,0.020132393,0.020606715,0.014308755,-0.0098751495,-0.00041853276,0.0061200894,-0.022609415,-0.0041173906,0.0043907855,-0.009196604,-0.004031749,0.0334398,0.020909756,-0.0021476308,0.020514486,0.00549754,-0.0063704266,-0.014651323,0.008610287,0.009341535,-0.02495468,0.0031769786,0.005464601,-0.017800303,-0.0029480516,-0.021436783,0.0040119854,0.023162792,-0.0073849517,0.0151651725,0.016825305,0.006818399,-0.00026330713,-0.00420962,-0.009315184,-0.028459404,0.012971427,0.014730376,0.0049902773,0.01895976,-0.009249306,0.0024094968,-0.02147631,0.018946584,0.0063243117,0.0025000796,-0.011337646,0.0071741412,0.003425669,-0.02819589,-0.007839511,0.0075364714,-0.019355029,-0.019157395,0.020646242,-0.002173982,0.040317487,0.009381062,-0.01635098,0.025969205,-0.01135741,0.01814287,0.0029233473,0.009058259,-0.006818399,-0.014190175,0.0066537033,-0.018788476,-0.00043273775,-0.009611636,-0.024981031,0.015468213,0.014914836,0.01793206,-0.028248593,0.0014888484,0.03509993,-0.025020558,0.026680691,-0.0013529746,-0.012220415,-0.006729463,0.027299946,0.006017978,-0.027695216,-0.0016255458,0.014137472,-0.00021472192,-0.009341535,-0.011291532,0.015731726,-0.027352648,0.0028294707,-0.005866458,0.0044665453,-0.007911977,-0.023452656,0.015059767,-0.019434083,-0.012088658,0.008320423,-0.015771253,-0.013808081,0.0058203433,-0.01765537]'


-- Set a session variable for the text search keyword (used in Hybrid Search).
SET myvars.keyword = 'dragon';

-- (Optional) Experiment with pgvector HNSW iterative scan settings.
-- strict_order: guarantees exact distance ordering (can be slower with filters)
-- relaxed_order: allows some relaxation for better performance
-- off: disables iterative scan
-- SET hnsw.iterative_scan = strict_order;
-- SET hnsw.iterative_scan = relaxed_order;
-- SET hnsw.iterative_scan = off;


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
LIMIT 41;


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
WHERE metadata->>'book_name' = '00. New Spring'
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 100;

-- Actual Query Configuration:
SELECT
    id,
    1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity,
    content,
    metadata
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring'
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 41;


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

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 20;

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 30;

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 40;

-- Critical boundary — documented tipping point for this dataset.
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 42;

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 50;

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 100;


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
LIMIT 40;

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_high')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 42;

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_high')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 50;


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

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name'            = current_setting('myvars.filter_compound_book')
  AND (metadata->>'chapter_number')::int = current_setting('myvars.filter_compound_chapter')::int
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 10;

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
