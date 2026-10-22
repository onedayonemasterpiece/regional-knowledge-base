-- Ordinary append-only SQL022. Source-scoped entity_list selectors before LIMIT.
-- Never rewrite SQL001-021 or the historical read_source_graph_v1 signature.
-- Exact Unicode default full casefold C+F mapping, pinned to Unicode 15.1
-- (CaseFolding-15.1.0.txt, unicode-org/icu release-74-2; 1530 mappings).
-- Unicode 15.0/15.1 share these mappings, covering CPython 3.12/3.13.
-- Do not substitute locale-dependent lower(), ILIKE, unaccent or NFKC.
CREATE FUNCTION rkb_v4.source_graph_casefold_v1(value text) RETURNS text
LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE SECURITY INVOKER SET search_path=pg_catalog AS $$
DECLARE folded text;
BEGIN
  folded := translate(value, 'ABCDEFGHIJKLMNOPQRSTUVWXYZµÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞĀĂĄĆĈĊČĎĐĒĔĖĘĚĜĞĠĢĤĦĨĪĬĮĲĴĶĹĻĽĿŁŃŅŇŊŌŎŐŒŔŖŘŚŜŞŠŢŤŦŨŪŬŮŰŲŴŶŸŹŻŽſƁƂƄƆƇƉƊƋƎƏƐƑƓƔƖƗƘƜƝƟƠƢƤƦƧƩƬƮƯƱƲƳƵƷƸƼǄǅǇǈǊǋǍǏǑǓǕǗǙǛǞǠǢǤǦǨǪǬǮǱǲǴǶǷǸǺǼǾȀȂȄȆȈȊȌȎȐȒȔȖȘȚȜȞȠȢȤȦȨȪȬȮȰȲȺȻȽȾɁɃɄɅɆɈɊɌɎͅͰͲͶͿΆΈΉΊΌΎΏΑΒΓΔΕΖΗΘΙΚΛΜΝΞΟΠΡΣΤΥΦΧΨΩΪΫςϏϐϑϕϖϘϚϜϞϠϢϤϦϨϪϬϮϰϱϴϵϷϹϺϽϾϿЀЁЂЃЄЅІЇЈЉЊЋЌЍЎЏАБВГДЕЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯѠѢѤѦѨѪѬѮѰѲѴѶѸѺѼѾҀҊҌҎҐҒҔҖҘҚҜҞҠҢҤҦҨҪҬҮҰҲҴҶҸҺҼҾӀӁӃӅӇӉӋӍӐӒӔӖӘӚӜӞӠӢӤӦӨӪӬӮӰӲӴӶӸӺӼӾԀԂԄԆԈԊԌԎԐԒԔԖԘԚԜԞԠԢԤԦԨԪԬԮԱԲԳԴԵԶԷԸԹԺԻԼԽԾԿՀՁՂՃՄՅՆՇՈՉՊՋՌՍՎՏՐՑՒՓՔՕՖႠႡႢႣႤႥႦႧႨႩႪႫႬႭႮႯႰႱႲႳႴႵႶႷႸႹႺႻႼႽႾႿჀჁჂჃჄჅჇჍᏸᏹᏺᏻᏼᏽᲀᲁᲂᲃᲄᲅᲆᲇᲈᲐᲑᲒᲓᲔᲕᲖᲗᲘᲙᲚᲛᲜᲝᲞᲟᲠᲡᲢᲣᲤᲥᲦᲧᲨᲩᲪᲫᲬᲭᲮᲯᲰᲱᲲᲳᲴᲵᲶᲷᲸᲹᲺᲽᲾᲿḀḂḄḆḈḊḌḎḐḒḔḖḘḚḜḞḠḢḤḦḨḪḬḮḰḲḴḶḸḺḼḾṀṂṄṆṈṊṌṎṐṒṔṖṘṚṜṞṠṢṤṦṨṪṬṮṰṲṴṶṸṺṼṾẀẂẄẆẈẊẌẎẐẒẔẛẠẢẤẦẨẪẬẮẰẲẴẶẸẺẼẾỀỂỄỆỈỊỌỎỐỒỔỖỘỚỜỞỠỢỤỦỨỪỬỮỰỲỴỶỸỺỼỾἈἉἊἋἌἍἎἏἘἙἚἛἜἝἨἩἪἫἬἭἮἯἸἹἺἻἼἽἾἿὈὉὊὋὌὍὙὛὝὟὨὩὪὫὬὭὮὯᾸᾹᾺΆιῈΈῊΉῘῙῚΊῨῩῪΎῬῸΌῺΏΩKÅℲⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩⅪⅫⅬⅭⅮⅯↃⒶⒷⒸⒹⒺⒻⒼⒽⒾⒿⓀⓁⓂⓃⓄⓅⓆⓇⓈⓉⓊⓋⓌⓍⓎⓏⰀⰁⰂⰃⰄⰅⰆⰇⰈⰉⰊⰋⰌⰍⰎⰏⰐⰑⰒⰓⰔⰕⰖⰗⰘⰙⰚⰛⰜⰝⰞⰟⰠⰡⰢⰣⰤⰥⰦⰧⰨⰩⰪⰫⰬⰭⰮⰯⱠⱢⱣⱤⱧⱩⱫⱭⱮⱯⱰⱲⱵⱾⱿⲀⲂⲄⲆⲈⲊⲌⲎⲐⲒⲔⲖⲘⲚⲜⲞⲠⲢⲤⲦⲨⲪⲬⲮⲰⲲⲴⲶⲸⲺⲼⲾⳀⳂⳄⳆⳈⳊⳌⳎⳐⳒⳔⳖⳘⳚⳜⳞⳠⳢⳫⳭⳲꙀꙂꙄꙆꙈꙊꙌꙎꙐꙒꙔꙖꙘꙚꙜꙞꙠꙢꙤꙦꙨꙪꙬꚀꚂꚄꚆꚈꚊꚌꚎꚐꚒꚔꚖꚘꚚꜢꜤꜦꜨꜪꜬꜮꜲꜴꜶꜸꜺꜼꜾꝀꝂꝄꝆꝈꝊꝌꝎꝐꝒꝔꝖꝘꝚꝜꝞꝠꝢꝤꝦꝨꝪꝬꝮꝹꝻꝽꝾꞀꞂꞄꞆꞋꞍꞐꞒꞖꞘꞚꞜꞞꞠꞢꞤꞦꞨꞪꞫꞬꞭꞮꞰꞱꞲꞳꞴꞶꞸꞺꞼꞾꟀꟂꟄꟅꟆꟇꟉꟐꟖꟘꟵꭰꭱꭲꭳꭴꭵꭶꭷꭸꭹꭺꭻꭼꭽꭾꭿꮀꮁꮂꮃꮄꮅꮆꮇꮈꮉꮊꮋꮌꮍꮎꮏꮐꮑꮒꮓꮔꮕꮖꮗꮘꮙꮚꮛꮜꮝꮞꮟꮠꮡꮢꮣꮤꮥꮦꮧꮨꮩꮪꮫꮬꮭꮮꮯꮰꮱꮲꮳꮴꮵꮶꮷꮸꮹꮺꮻꮼꮽꮾꮿＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ𐐀𐐁𐐂𐐃𐐄𐐅𐐆𐐇𐐈𐐉𐐊𐐋𐐌𐐍𐐎𐐏𐐐𐐑𐐒𐐓𐐔𐐕𐐖𐐗𐐘𐐙𐐚𐐛𐐜𐐝𐐞𐐟𐐠𐐡𐐢𐐣𐐤𐐥𐐦𐐧𐒰𐒱𐒲𐒳𐒴𐒵𐒶𐒷𐒸𐒹𐒺𐒻𐒼𐒽𐒾𐒿𐓀𐓁𐓂𐓃𐓄𐓅𐓆𐓇𐓈𐓉𐓊𐓋𐓌𐓍𐓎𐓏𐓐𐓑𐓒𐓓𐕰𐕱𐕲𐕳𐕴𐕵𐕶𐕷𐕸𐕹𐕺𐕼𐕽𐕾𐕿𐖀𐖁𐖂𐖃𐖄𐖅𐖆𐖇𐖈𐖉𐖊𐖌𐖍𐖎𐖏𐖐𐖑𐖒𐖔𐖕𐲀𐲁𐲂𐲃𐲄𐲅𐲆𐲇𐲈𐲉𐲊𐲋𐲌𐲍𐲎𐲏𐲐𐲑𐲒𐲓𐲔𐲕𐲖𐲗𐲘𐲙𐲚𐲛𐲜𐲝𐲞𐲟𐲠𐲡𐲢𐲣𐲤𐲥𐲦𐲧𐲨𐲩𐲪𐲫𐲬𐲭𐲮𐲯𐲰𐲱𐲲𑢠𑢡𑢢𑢣𑢤𑢥𑢦𑢧𑢨𑢩𑢪𑢫𑢬𑢭𑢮𑢯𑢰𑢱𑢲𑢳𑢴𑢵𑢶𑢷𑢸𑢹𑢺𑢻𑢼𑢽𑢾𑢿𖹀𖹁𖹂𖹃𖹄𖹅𖹆𖹇𖹈𖹉𖹊𖹋𖹌𖹍𖹎𖹏𖹐𖹑𖹒𖹓𖹔𖹕𖹖𖹗𖹘𖹙𖹚𖹛𖹜𖹝𖹞𖹟𞤀𞤁𞤂𞤃𞤄𞤅𞤆𞤇𞤈𞤉𞤊𞤋𞤌𞤍𞤎𞤏𞤐𞤑𞤒𞤓𞤔𞤕𞤖𞤗𞤘𞤙𞤚𞤛𞤜𞤝𞤞𞤟𞤠𞤡', 'abcdefghijklmnopqrstuvwxyzμàáâãäåæçèéêëìíîïðñòóôõöøùúûüýþāăąćĉċčďđēĕėęěĝğġģĥħĩīĭįĳĵķĺļľŀłńņňŋōŏőœŕŗřśŝşšţťŧũūŭůűųŵŷÿźżžsɓƃƅɔƈɖɗƌǝəɛƒɠɣɩɨƙɯɲɵơƣƥʀƨʃƭʈưʊʋƴƶʒƹƽǆǆǉǉǌǌǎǐǒǔǖǘǚǜǟǡǣǥǧǩǫǭǯǳǳǵƕƿǹǻǽǿȁȃȅȇȉȋȍȏȑȓȕȗșțȝȟƞȣȥȧȩȫȭȯȱȳⱥȼƚⱦɂƀʉʌɇɉɋɍɏιͱͳͷϳάέήίόύώαβγδεζηθικλμνξοπρστυφχψωϊϋσϗβθφπϙϛϝϟϡϣϥϧϩϫϭϯκρθεϸϲϻͻͼͽѐёђѓєѕіїјљњћќѝўџабвгдежзийклмнопрстуфхцчшщъыьэюяѡѣѥѧѩѫѭѯѱѳѵѷѹѻѽѿҁҋҍҏґғҕҗҙқҝҟҡңҥҧҩҫҭүұҳҵҷҹһҽҿӏӂӄӆӈӊӌӎӑӓӕӗәӛӝӟӡӣӥӧөӫӭӯӱӳӵӷӹӻӽӿԁԃԅԇԉԋԍԏԑԓԕԗԙԛԝԟԡԣԥԧԩԫԭԯաբգդեզէըթժիլխծկհձղճմյնշոչպջռսվտրցւփքօֆⴀⴁⴂⴃⴄⴅⴆⴇⴈⴉⴊⴋⴌⴍⴎⴏⴐⴑⴒⴓⴔⴕⴖⴗⴘⴙⴚⴛⴜⴝⴞⴟⴠⴡⴢⴣⴤⴥⴧⴭᏰᏱᏲᏳᏴᏵвдосттъѣꙋაბგდევზთიკლმნოპჟრსტუფქღყშჩცძწჭხჯჰჱჲჳჴჵჶჷჸჹჺჽჾჿḁḃḅḇḉḋḍḏḑḓḕḗḙḛḝḟḡḣḥḧḩḫḭḯḱḳḵḷḹḻḽḿṁṃṅṇṉṋṍṏṑṓṕṗṙṛṝṟṡṣṥṧṩṫṭṯṱṳṵṷṹṻṽṿẁẃẅẇẉẋẍẏẑẓẕṡạảấầẩẫậắằẳẵặẹẻẽếềểễệỉịọỏốồổỗộớờởỡợụủứừửữựỳỵỷỹỻỽỿἀἁἂἃἄἅἆἇἐἑἒἓἔἕἠἡἢἣἤἥἦἧἰἱἲἳἴἵἶἷὀὁὂὃὄὅὑὓὕὗὠὡὢὣὤὥὦὧᾰᾱὰάιὲέὴήῐῑὶίῠῡὺύῥὸόὼώωkåⅎⅰⅱⅲⅳⅴⅵⅶⅷⅸⅹⅺⅻⅼⅽⅾⅿↄⓐⓑⓒⓓⓔⓕⓖⓗⓘⓙⓚⓛⓜⓝⓞⓟⓠⓡⓢⓣⓤⓥⓦⓧⓨⓩⰰⰱⰲⰳⰴⰵⰶⰷⰸⰹⰺⰻⰼⰽⰾⰿⱀⱁⱂⱃⱄⱅⱆⱇⱈⱉⱊⱋⱌⱍⱎⱏⱐⱑⱒⱓⱔⱕⱖⱗⱘⱙⱚⱛⱜⱝⱞⱟⱡɫᵽɽⱨⱪⱬɑɱɐɒⱳⱶȿɀⲁⲃⲅⲇⲉⲋⲍⲏⲑⲓⲕⲗⲙⲛⲝⲟⲡⲣⲥⲧⲩⲫⲭⲯⲱⲳⲵⲷⲹⲻⲽⲿⳁⳃⳅⳇⳉⳋⳍⳏⳑⳓⳕⳗⳙⳛⳝⳟⳡⳣⳬⳮⳳꙁꙃꙅꙇꙉꙋꙍꙏꙑꙓꙕꙗꙙꙛꙝꙟꙡꙣꙥꙧꙩꙫꙭꚁꚃꚅꚇꚉꚋꚍꚏꚑꚓꚕꚗꚙꚛꜣꜥꜧꜩꜫꜭꜯꜳꜵꜷꜹꜻꜽꜿꝁꝃꝅꝇꝉꝋꝍꝏꝑꝓꝕꝗꝙꝛꝝꝟꝡꝣꝥꝧꝩꝫꝭꝯꝺꝼᵹꝿꞁꞃꞅꞇꞌɥꞑꞓꞗꞙꞛꞝꞟꞡꞣꞥꞧꞩɦɜɡɬɪʞʇʝꭓꞵꞷꞹꞻꞽꞿꟁꟃꞔʂᶎꟈꟊꟑꟗꟙꟶᎠᎡᎢᎣᎤᎥᎦᎧᎨᎩᎪᎫᎬᎭᎮᎯᎰᎱᎲᎳᎴᎵᎶᎷᎸᎹᎺᎻᎼᎽᎾᎿᏀᏁᏂᏃᏄᏅᏆᏇᏈᏉᏊᏋᏌᏍᏎᏏᏐᏑᏒᏓᏔᏕᏖᏗᏘᏙᏚᏛᏜᏝᏞᏟᏠᏡᏢᏣᏤᏥᏦᏧᏨᏩᏪᏫᏬᏭᏮᏯａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ𐐨𐐩𐐪𐐫𐐬𐐭𐐮𐐯𐐰𐐱𐐲𐐳𐐴𐐵𐐶𐐷𐐸𐐹𐐺𐐻𐐼𐐽𐐾𐐿𐑀𐑁𐑂𐑃𐑄𐑅𐑆𐑇𐑈𐑉𐑊𐑋𐑌𐑍𐑎𐑏𐓘𐓙𐓚𐓛𐓜𐓝𐓞𐓟𐓠𐓡𐓢𐓣𐓤𐓥𐓦𐓧𐓨𐓩𐓪𐓫𐓬𐓭𐓮𐓯𐓰𐓱𐓲𐓳𐓴𐓵𐓶𐓷𐓸𐓹𐓺𐓻𐖗𐖘𐖙𐖚𐖛𐖜𐖝𐖞𐖟𐖠𐖡𐖣𐖤𐖥𐖦𐖧𐖨𐖩𐖪𐖫𐖬𐖭𐖮𐖯𐖰𐖱𐖳𐖴𐖵𐖶𐖷𐖸𐖹𐖻𐖼𐳀𐳁𐳂𐳃𐳄𐳅𐳆𐳇𐳈𐳉𐳊𐳋𐳌𐳍𐳎𐳏𐳐𐳑𐳒𐳓𐳔𐳕𐳖𐳗𐳘𐳙𐳚𐳛𐳜𐳝𐳞𐳟𐳠𐳡𐳢𐳣𐳤𐳥𐳦𐳧𐳨𐳩𐳪𐳫𐳬𐳭𐳮𐳯𐳰𐳱𐳲𑣀𑣁𑣂𑣃𑣄𑣅𑣆𑣇𑣈𑣉𑣊𑣋𑣌𑣍𑣎𑣏𑣐𑣑𑣒𑣓𑣔𑣕𑣖𑣗𑣘𑣙𑣚𑣛𑣜𑣝𑣞𑣟𖹠𖹡𖹢𖹣𖹤𖹥𖹦𖹧𖹨𖹩𖹪𖹫𖹬𖹭𖹮𖹯𖹰𖹱𖹲𖹳𖹴𖹵𖹶𖹷𖹸𖹹𖹺𖹻𖹼𖹽𖹾𖹿𞤢𞤣𞤤𞤥𞤦𞤧𞤨𞤩𞤪𞤫𞤬𞤭𞤮𞤯𞤰𞤱𞤲𞤳𞤴𞤵𞤶𞤷𞤸𞤹𞤺𞤻𞤼𞤽𞤾𞤿𞥀𞥁𞥂𞥃');
  -- Expansions are rare; skip the fixed 104 literal replacements otherwise.
  IF translate(folded, 'ßİŉǰΐΰևẖẗẘẙẚẞὐὒὔὖᾀᾁᾂᾃᾄᾅᾆᾇᾈᾉᾊᾋᾌᾍᾎᾏᾐᾑᾒᾓᾔᾕᾖᾗᾘᾙᾚᾛᾜᾝᾞᾟᾠᾡᾢᾣᾤᾥᾦᾧᾨᾩᾪᾫᾬᾭᾮᾯᾲᾳᾴᾶᾷᾼῂῃῄῆῇῌῒΐῖῗῢΰῤῦῧῲῳῴῶῷῼﬀﬁﬂﬃﬄﬅﬆﬓﬔﬕﬖﬗ', '') <> folded THEN
    folded := replace(folded, 'ß', 'ss');
    folded := replace(folded, 'İ', 'i̇');
    folded := replace(folded, 'ŉ', 'ʼn');
    folded := replace(folded, 'ǰ', 'ǰ');
    folded := replace(folded, 'ΐ', 'ΐ');
    folded := replace(folded, 'ΰ', 'ΰ');
    folded := replace(folded, 'և', 'եւ');
    folded := replace(folded, 'ẖ', 'ẖ');
    folded := replace(folded, 'ẗ', 'ẗ');
    folded := replace(folded, 'ẘ', 'ẘ');
    folded := replace(folded, 'ẙ', 'ẙ');
    folded := replace(folded, 'ẚ', 'aʾ');
    folded := replace(folded, 'ẞ', 'ss');
    folded := replace(folded, 'ὐ', 'ὐ');
    folded := replace(folded, 'ὒ', 'ὒ');
    folded := replace(folded, 'ὔ', 'ὔ');
    folded := replace(folded, 'ὖ', 'ὖ');
    folded := replace(folded, 'ᾀ', 'ἀι');
    folded := replace(folded, 'ᾁ', 'ἁι');
    folded := replace(folded, 'ᾂ', 'ἂι');
    folded := replace(folded, 'ᾃ', 'ἃι');
    folded := replace(folded, 'ᾄ', 'ἄι');
    folded := replace(folded, 'ᾅ', 'ἅι');
    folded := replace(folded, 'ᾆ', 'ἆι');
    folded := replace(folded, 'ᾇ', 'ἇι');
    folded := replace(folded, 'ᾈ', 'ἀι');
    folded := replace(folded, 'ᾉ', 'ἁι');
    folded := replace(folded, 'ᾊ', 'ἂι');
    folded := replace(folded, 'ᾋ', 'ἃι');
    folded := replace(folded, 'ᾌ', 'ἄι');
    folded := replace(folded, 'ᾍ', 'ἅι');
    folded := replace(folded, 'ᾎ', 'ἆι');
    folded := replace(folded, 'ᾏ', 'ἇι');
    folded := replace(folded, 'ᾐ', 'ἠι');
    folded := replace(folded, 'ᾑ', 'ἡι');
    folded := replace(folded, 'ᾒ', 'ἢι');
    folded := replace(folded, 'ᾓ', 'ἣι');
    folded := replace(folded, 'ᾔ', 'ἤι');
    folded := replace(folded, 'ᾕ', 'ἥι');
    folded := replace(folded, 'ᾖ', 'ἦι');
    folded := replace(folded, 'ᾗ', 'ἧι');
    folded := replace(folded, 'ᾘ', 'ἠι');
    folded := replace(folded, 'ᾙ', 'ἡι');
    folded := replace(folded, 'ᾚ', 'ἢι');
    folded := replace(folded, 'ᾛ', 'ἣι');
    folded := replace(folded, 'ᾜ', 'ἤι');
    folded := replace(folded, 'ᾝ', 'ἥι');
    folded := replace(folded, 'ᾞ', 'ἦι');
    folded := replace(folded, 'ᾟ', 'ἧι');
    folded := replace(folded, 'ᾠ', 'ὠι');
    folded := replace(folded, 'ᾡ', 'ὡι');
    folded := replace(folded, 'ᾢ', 'ὢι');
    folded := replace(folded, 'ᾣ', 'ὣι');
    folded := replace(folded, 'ᾤ', 'ὤι');
    folded := replace(folded, 'ᾥ', 'ὥι');
    folded := replace(folded, 'ᾦ', 'ὦι');
    folded := replace(folded, 'ᾧ', 'ὧι');
    folded := replace(folded, 'ᾨ', 'ὠι');
    folded := replace(folded, 'ᾩ', 'ὡι');
    folded := replace(folded, 'ᾪ', 'ὢι');
    folded := replace(folded, 'ᾫ', 'ὣι');
    folded := replace(folded, 'ᾬ', 'ὤι');
    folded := replace(folded, 'ᾭ', 'ὥι');
    folded := replace(folded, 'ᾮ', 'ὦι');
    folded := replace(folded, 'ᾯ', 'ὧι');
    folded := replace(folded, 'ᾲ', 'ὰι');
    folded := replace(folded, 'ᾳ', 'αι');
    folded := replace(folded, 'ᾴ', 'άι');
    folded := replace(folded, 'ᾶ', 'ᾶ');
    folded := replace(folded, 'ᾷ', 'ᾶι');
    folded := replace(folded, 'ᾼ', 'αι');
    folded := replace(folded, 'ῂ', 'ὴι');
    folded := replace(folded, 'ῃ', 'ηι');
    folded := replace(folded, 'ῄ', 'ήι');
    folded := replace(folded, 'ῆ', 'ῆ');
    folded := replace(folded, 'ῇ', 'ῆι');
    folded := replace(folded, 'ῌ', 'ηι');
    folded := replace(folded, 'ῒ', 'ῒ');
    folded := replace(folded, 'ΐ', 'ΐ');
    folded := replace(folded, 'ῖ', 'ῖ');
    folded := replace(folded, 'ῗ', 'ῗ');
    folded := replace(folded, 'ῢ', 'ῢ');
    folded := replace(folded, 'ΰ', 'ΰ');
    folded := replace(folded, 'ῤ', 'ῤ');
    folded := replace(folded, 'ῦ', 'ῦ');
    folded := replace(folded, 'ῧ', 'ῧ');
    folded := replace(folded, 'ῲ', 'ὼι');
    folded := replace(folded, 'ῳ', 'ωι');
    folded := replace(folded, 'ῴ', 'ώι');
    folded := replace(folded, 'ῶ', 'ῶ');
    folded := replace(folded, 'ῷ', 'ῶι');
    folded := replace(folded, 'ῼ', 'ωι');
    folded := replace(folded, 'ﬀ', 'ff');
    folded := replace(folded, 'ﬁ', 'fi');
    folded := replace(folded, 'ﬂ', 'fl');
    folded := replace(folded, 'ﬃ', 'ffi');
    folded := replace(folded, 'ﬄ', 'ffl');
    folded := replace(folded, 'ﬅ', 'st');
    folded := replace(folded, 'ﬆ', 'st');
    folded := replace(folded, 'ﬓ', 'մն');
    folded := replace(folded, 'ﬔ', 'մե');
    folded := replace(folded, 'ﬕ', 'մի');
    folded := replace(folded, 'ﬖ', 'վն');
    folded := replace(folded, 'ﬗ', 'մխ');
  END IF;
  RETURN folded;
END $$;

-- Only the preexisting authorized source-graph tables and receipt joins.
-- All source, revision, visibility, withdrawal and C-collated keyset fences
-- are identical to SQL011 read_source_graph_v1. No additional corpus scope.
-- SQL input is data, not dynamic SQL or a LIKE pattern.
CREATE FUNCTION rkb_v4.read_source_graph_filtered_v1(
 wanted_source uuid, after_source uuid, after_key text, selector_kinds text[],
 folded_query text, take integer)
RETURNS TABLE(source_id uuid,source_revision bigint,eligibility_revision bigint,
 record_id text,data jsonb,evidence_text text,bibliography text)
LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog AS $$
  SELECT n.source_id,1::bigint,v.eligibility_revision,n.entity_key,n.data,
    n.data->'mention'->'evidence'->>'exact_quote',s.bibliography
  FROM rkb_v4.source_entity_representations n
  JOIN rkb_v4.sources s USING(source_id)
  JOIN rkb_v4.active_revision active USING(source_id)
  JOIN rkb_v4.source_visibility v USING(source_id)
  JOIN rkb_v4.source_graph_receipts receipt USING(source_id)
  WHERE wanted_source IS NOT NULL AND n.source_id=wanted_source
    AND s.accepted AND NOT s.withdrawn AND active.revision=1
    AND (after_source IS NULL OR n.source_id>after_source
      OR n.source_id=after_source AND n.entity_key COLLATE "C">after_key COLLATE "C")
    AND (selector_kinds IS NULL OR n.data->>'kind'=ANY(selector_kinds))
    AND (folded_query IS NULL OR position(folded_query IN rkb_v4.source_graph_casefold_v1(n.data->>'canonical_label'))>0
      OR position(folded_query IN rkb_v4.source_graph_casefold_v1(n.data->'mention'->>'exact_source_spelling'))>0
      OR EXISTS (
        SELECT 1 FROM jsonb_array_elements(n.data->'aliases') alias(value)
        WHERE jsonb_typeof(alias.value->'value')='string'
          AND position(folded_query IN rkb_v4.source_graph_casefold_v1(alias.value->>'value'))>0))
  ORDER BY n.source_id,n.entity_key COLLATE "C"
  LIMIT least(greatest(take,1),4)
$$;

REVOKE ALL ON FUNCTION rkb_v4.source_graph_casefold_v1(text),
 rkb_v4.read_source_graph_filtered_v1(uuid,uuid,text,text[],text,integer) FROM PUBLIC;
-- No new role, table privileges or audience. Reuse only an existing reader
-- already authorized to execute the SQL011 source-graph read.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='rkb_reader') THEN
    IF has_function_privilege('rkb_reader',
        'rkb_v4.read_source_graph_v1(uuid,uuid,text,uuid,text,integer)','EXECUTE') THEN
      GRANT EXECUTE ON FUNCTION rkb_v4.source_graph_casefold_v1(text),
       rkb_v4.read_source_graph_filtered_v1(uuid,uuid,text,text[],text,integer) TO rkb_reader;
    END IF;
  END IF;
END $$;
