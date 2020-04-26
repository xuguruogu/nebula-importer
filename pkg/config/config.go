package config

import (
	"errors"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"

	"github.com/vesoft-inc/nebula-importer/pkg/base"
	"github.com/vesoft-inc/nebula-importer/pkg/logger"
	yaml "gopkg.in/yaml.v2"
)

type NebulaClientConnection struct {
	User     *string `json:"user" yaml:"user"`
	Password *string `json:"password" yaml:"password"`
	Address  *string `json:"address" yaml:"address"`
}

type NebulaClientSettings struct {
	Retry             *int                    `json:"retry" yaml:"retry"`
	Concurrency       *int                    `json:"concurrency" yaml:"concurrency"`
	ChannelBufferSize *int                    `json:"channelBufferSize" yaml:"channelBufferSize"`
	Space             *string                 `json:"space" yaml:"space"`
	Connection        *NebulaClientConnection `json:"connection" yaml:"connection"`
}

type Prop struct {
	Name  *string `json:"name" yaml:"name"`
	Type  *string `json:"type" yaml:"type"`
	Index *int    `json:"index" yaml:"index"`
}

type VID struct {
	Index    *int    `json:"index" yaml:"index"`
	Function *string `json:"function" yaml:"function"`
}

type Rank struct {
	Index *int `json:"index" yaml:"index"`
}

type Edge struct {
	Name        *string `json:"name" yaml:"name"`
	WithRanking *bool   `json:"withRanking" yaml:"withRanking"`
	Props       []*Prop `json:"props" yaml:"props"`
	SrcVID      *VID    `json:"srcVID" yaml:"srcVID"`
	DstVID      *VID    `json:"dstVID" yaml:"dstVID"`
	Rank        *Rank   `json:"rank" yaml:"rank"`
}

type Tag struct {
	Name  *string `json:"name" yaml:"name"`
	Props []*Prop `json:"props" yaml:"props"`
}

type Vertex struct {
	VID  *VID   `json:"vid" yaml:"vid"`
	Tags []*Tag `json:"tags" yaml:"tags"`
}

type Schema struct {
	Type   *string `json:"type" yaml:"type"`
	Edge   *Edge   `json:"edge" yaml:"edge"`
	Vertex *Vertex `json:"vertex" yaml:"vertex"`
}

type CSVConfig struct {
	WithHeader *bool   `json:"withHeader" yaml:"withHeader"`
	WithLabel  *bool   `json:"withLabel" yaml:"withLabel"`
	Delimiter  *string `json:"delimiter" yaml:"delimiter"`
}

type File struct {
	Paths        []string
	Path         *string    `json:"path" yaml:"path"`
	FailDataPath *string    `json:"failDataPath" yaml:"failDataPath"`
	BatchSize    *int       `json:"batchSize" yaml:"batchSize"`
	Limit        *int       `json:"limit" yaml:"limit"`
	InOrder      *bool      `json:"inOrder" yaml:"inOrder"`
	Type         *string    `json:"type" yaml:"type"`
	CSV          *CSVConfig `json:"csv" yaml:"csv"`
	Schema       *Schema    `json:"schema" yaml:"schema"`
}

type YAMLConfig struct {
	Version              *string               `json:"version" yaml:"version"`
	Description          *string               `json:"description" yaml:"description"`
	NebulaClientSettings *NebulaClientSettings `json:"clientSettings" yaml:"clientSettings"`
	LogPath              *string               `json:"logPath" yaml:"logPath"`
	Files                []*File               `json:"files" yaml:"files"`
}

var version string = "v1rc2"

func Parse(filename string) (*YAMLConfig, error) {
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var conf YAMLConfig
	if err = yaml.Unmarshal(content, &conf); err != nil {
		return nil, err
	}

	if conf.Version == nil && *conf.Version != version {
		return nil, fmt.Errorf("The YAML configure version must be %s", version)
	}

	path, err := filepath.Abs(filepath.Dir(filename))
	if err != nil {
		return nil, err
	}
	if err = conf.ValidateAndReset(path); err != nil {
		return nil, err
	}

	return &conf, nil
}

func (config *YAMLConfig) ValidateAndReset(dir string) error {
	if config.NebulaClientSettings == nil {
		return errors.New("please configure clientSettings")
	}
	if err := config.NebulaClientSettings.validateAndReset("clientSettings"); err != nil {
		return err
	}

	if config.LogPath == nil {
		defaultPath := "/tmp/nebula-importer.log"
		config.LogPath = &defaultPath
		logger.Warnf("You have not configured the log file path in: logPath, reset to default path: %s", *config.LogPath)
	}

	if config.Files == nil || len(config.Files) == 0 {
		return errors.New("There is no files in configuration")
	}

	for i := range config.Files {
		if err := config.Files[i].validateAndReset(dir, fmt.Sprintf("files[%d]", i)); err != nil {
			return err
		}
	}

	return nil
}

func (n *NebulaClientSettings) validateAndReset(prefix string) error {
	if n.Space == nil {
		return fmt.Errorf("Please configure the space name in: %s.space", prefix)
	}

	if n.Retry == nil {
		retry := 1
		n.Retry = &retry
		logger.Warnf("Invalid retry option in %s.retry, reset to %d ", prefix, *n.Retry)
	}

	if n.Concurrency == nil {
		d := 10
		n.Concurrency = &d
		logger.Warnf("Invalid client concurrency in %s.concurrency, reset to %d", prefix, *n.Concurrency)
	}

	if n.ChannelBufferSize == nil {
		d := 128
		n.ChannelBufferSize = &d
		logger.Warnf("Invalid client channel buffer size in %s.channelBufferSize, reset to %d", prefix, *n.ChannelBufferSize)
	}

	if n.Connection == nil {
		return fmt.Errorf("Please configure the connection information in: %s.connection", prefix)
	} else {
		return n.Connection.validateAndReset(fmt.Sprintf("%s.connection", prefix))
	}
}

func (c *NebulaClientConnection) validateAndReset(prefix string) error {
	if c.Address == nil {
		a := "127.0.0.1:3699"
		c.Address = &a
		logger.Warnf("%s.address: %s", prefix, *c.Address)
	}

	if c.User == nil {
		u := "user"
		c.User = &u
		logger.Warnf("%s.user: %s", prefix, *c.User)
	}

	if c.Password == nil {
		p := "password"
		c.Password = &p
		logger.Warnf("%s.password: %s", prefix, *c.Password)
	}
	return nil
}

func (f *File) validateAndReset(dir, prefix string) error {
	if f.Path == nil {
		return fmt.Errorf("Please configure file path in: %s.path", prefix)
	}
	if !base.PathExists(*f.Path) {
		path := filepath.Join(dir, *f.Path)
		if !base.PathExists(path) {
			return fmt.Errorf("File(%s) doesn't exist", *f.Path)
		} else {
			f.Path = &path
		}
	}
	f.Paths, _ = base.PathFileList(*f.Path)

	if f.FailDataPath == nil {
		if d, err := filepath.Abs(filepath.Dir(*f.Path)); err != nil {
			return err
		} else {
			p := filepath.Join(d, "err", filepath.Base(*f.Path))
			f.FailDataPath = &p
			logger.Warnf("You have not configured the failed data output file path in: %s.failDataPath, reset to default path: %s", prefix, *f.FailDataPath)
		}
	}
	if f.BatchSize == nil {
		b := 128
		f.BatchSize = &b
		logger.Infof("Invalid batch size in path(%s), reset to %d", *f.Path, *f.BatchSize)
	}
	if f.InOrder == nil {
		inOrder := false
		f.InOrder = &inOrder
	}
	if strings.ToLower(*f.Type) != "csv" {
		// TODO: Now only support csv import
		return fmt.Errorf("Invalid file data type: %s, reset to csv", *f.Type)
	}

	if f.CSV != nil {
		err := f.CSV.validateAndReset(fmt.Sprintf("%s.csv", prefix))
		if err != nil {
			return err
		}
	}

	if f.Schema == nil {
		return fmt.Errorf("Please configure file schema: %s.schema", prefix)
	}
	return f.Schema.validateAndReset(fmt.Sprintf("%s.schema", prefix))
}

func (c *CSVConfig) validateAndReset(prefix string) error {
	if c.WithHeader == nil {
		h := false
		c.WithHeader = &h
		logger.Infof("%s.withHeader: %v", prefix, false)
	}

	if c.WithLabel == nil {
		l := false
		c.WithLabel = &l
		logger.Infof("%s.withLabel: %v", prefix, false)
	}

	if c.Delimiter != nil {
		if len(*c.Delimiter) == 0 {
			return fmt.Errorf("%s.delimiter is empty string", prefix)
		}
	}

	return nil
}

func (s *Schema) IsVertex() bool {
	return strings.ToUpper(*s.Type) == "VERTEX"
}

func (s *Schema) String() string {
	if s.IsVertex() {
		return s.Vertex.String()
	} else {
		return s.Edge.String()
	}
}

func (s *Schema) validateAndReset(prefix string) error {
	var err error = nil
	switch strings.ToLower(*s.Type) {
	case "edge":
		if s.Edge != nil {
			err = s.Edge.validateAndReset(fmt.Sprintf("%s.edge", prefix))
		} else {
			logger.Infof("%s.edge is nil", prefix)
		}
	case "vertex":
		if s.Vertex != nil {
			err = s.Vertex.validateAndReset(fmt.Sprintf("%s.vertex", prefix))
		} else {
			logger.Infof("%s.vertex is nil", prefix)
		}
	default:
		err = fmt.Errorf("Error schema type(%s) in %s.type only edge and vertex are supported", *s.Type, prefix)
	}
	return err
}

func (v *VID) ParseFunction(str string) {
	i := strings.Index(str, "(")
	j := strings.Index(str, ")")
	if i < 0 && j < 0 {
		v.Function = nil
	} else if i > 0 && j > i {
		function := strings.ToLower(str[i+1 : j])
		v.Function = &function
	} else {
		logger.Fatalf("Invalid function format: %s", str)
	}
}

func (v *VID) String(vid string) string {
	if v.Function == nil || *v.Function == "" {
		return vid
	} else {
		return fmt.Sprintf("%s(%s)", vid, *v.Function)
	}
}

func (v *VID) checkFunction(prefix string) error {
	if v.Function != nil {
		switch strings.ToLower(*v.Function) {
		case "", "hash", "uuid":
		default:
			return fmt.Errorf("Invalid %s.function: %s, only following values are supported: \"\", hash, uuid", prefix, *v.Function)
		}
	}
	return nil
}

func (v *VID) validateAndReset(prefix string, defaultVal int) error {
	if v.Index == nil {
		v.Index = &defaultVal
	}
	if *v.Index < 0 {
		return fmt.Errorf("Invalid %s.index: %d", prefix, *v.Index)
	}
	if err := v.checkFunction(prefix); err != nil {
		return err
	}
	return nil
}

func (r *Rank) validateAndReset(prefix string, defaultVal int) error {
	if r.Index == nil {
		r.Index = &defaultVal
	}
	if *r.Index < 0 {
		return fmt.Errorf("Invalid %s.index: %d", prefix, *r.Index)
	}
	return nil
}

func (e *Edge) FormatValues(record base.Record) string {
	rank := ""
	if e.Rank != nil && e.Rank.Index != nil {
		rank = fmt.Sprintf("@%s", record[*e.Rank.Index])
	}
	var srcVID string
	if e.SrcVID.Function != nil {
		//TODO(yee): differentiate string and integer column type, find and compare src/dst vertex column with property
		srcVID = fmt.Sprintf("%s(%q)", *e.SrcVID.Function, record[*e.SrcVID.Index])
	} else {
		srcVID = base.TryConvInt64(record[*e.SrcVID.Index])
	}

	// 973982126#0.754774&1129254720#0.748774&1218438640#0.737752&1082845006#0.729269&1881749655#0.719917&2953254509#0.708333&169906135#0.687367&1799168637#0.683112&1269355681#0.676985&2953008720#0.663827&1670574741#0.663549&1483016328#0.662665&2264251534#0.656445&2213286429#0.650331&2847307720#0.648758&2258389336#0.645847&1491021680#0.642345&1254750210#0.635642&2704552328#0.632694&1276928541#0.628406&2698856383#0.628107&2319183044#0.622265&1194253705#0.616975&2322958743#0.610809&3408968146#0.592108&2847864283#0.589921&677980161#0.57684&3180214368#0.573984&971442969#0.573887&2378552760#0.568896&2221974103#0.560886&1102828600#0.557258&1662070201#0.55409&243338320#0.554002&556222159#0.551627&2483083801#0.551267&2110318521#0.545812&1735750726#0.545189&968933426#0.540172&2885107106#0.539484&145623555#0.536697&647545300#0.528941&719588215#0.52858&888204230#0.524923&2068187363#0.521402&3410081558#0.514841&789012521#0.514259&189969075#0.510063&2051145037#0.509478&2952010084#0.509048&1618995460#0.508766&1488797942#0.506299&472267960#0.505224&1693364917#0.501976&1538022780#0.497418&359481188#0.496217&1083636822#0.495179&2178556722#0.494518&1673919263#0.494099&445499760#0.487704&2439603860#0.487053&1525167005#0.486629&2647889537#0.486375&871626560#0.485912&1029369306#0.479715&1834618013#0.478561&1162008022#0.476461&1617604122#0.476425&27283977#0.475094&938170331#0.474612&1317948181#0.474092&2942250912#0.471607&243963875#0.468387&1353615006#0.467606&1664453788#0.465394&1911192136#0.464742&996775984#0.463629&662410704#0.460985&1972248481#0.460483&2404097338#0.459854&347598723#0.459341&1712305082#0.458683&1445522861#0.454785&2524577735#0.454674&1178134406#0.451427&495715520#0.447705&3041085740#0.446755&2078961710#0.444962&1342533832#0.444088&2255236343#0.441859&1663173761#0.439329&107141866#0.438532&2528831461#0.438468&6339551#0.436179&831321820#0.436096&703662222#0.433972&56528681#0.430558&45067232#0.429872&1787911938#0.42848&1915252803#0.428164&1287256620#0.427238&2288884109#0.425546&1093303241#0.423688&1240955563#0.419582&2828901404#0.418008&1421493669#0.417802&2346505530#0.41618&3356261565#0.413881&2449447320#0.408355&2800661783#0.406876&2614894323#0.405177&2797143518#0.405062&2464218982#0.399541&2770461239#0.396599&1233904513#0.392169&386585610#0.391234&1585158543#0.390037&1095396242#0.389631&331346696#0.388556&825303142#0.38822&1035011400#0.387556&536377020#0.384672&865986320#0.379909&2338554638#0.379889&1268456502#0.379765&2127695340#0.375036&2315911761#0.373509&2974689400#0.371054&2410215812#0.370943&76425005#0.37082&269085886#0.368411&1742191180#0.366126&1855719423#0.361407&1013847762#0.359703&635103882#0.358537&615442177#0.354094&1210673540#0.353267&2114302443#0.346315&2281606912#0.344644&3498703664#0.343453&1669668103#0.342714&1853376543#0.342409&1429128711#0.341153&2745832681#0.33936&2930851080#0.335834&2718083860#0.333307&899376240#0.332366&2359237940#0.331461&2438329217#0.329545&400839072#0.329281&987283904#0.32788&728746929#0.326745&2205276322#0.325251&1902858998#0.323418&1844633240#0.320182&2429776382#0.312844&1368891412#0.31062&1580275438#0.310098&881461300#0.307861&2630771116#0.304774&2249191430#0.29941&248698074#0.298773&676519524#0.298625&2660709714#0.297486&1499474402#0.295563&1775142801#0.29222&2148882020#0.292182&370238120#0.289497&946422700#0.289076&1679019043#0.286827&1304106901#0.286283&936051076#0.28614&2419569022#0.283895&2220212608#0.283663&181220135#0.282642&1373612782#0.276758&250797801#0.275641&2536682666#0.27414&1929422941#0.272866&2222966523#0.264492&1171418156#0.263674&215165175#0.262386&863593300#0.26174&345859195#0.261673&2882377602#0.261259&1242872840#0.258054&149504855#0.254869&508009255#0.245666&1139634918#0.244703&1551913209#0.244486&557913804#0.241673&1026421531#0.238943&3113035017#0.238311&2739754619#0.237624&535403563#0.237375&2168575036#0.231755&2073198661#0.231073&465178518#0.22906&480483760#0.228752&2833410540#0.228441&2809496741#0.227404&2982397319#0.22712&1120044590#0.226379&1474605410#0.226061&1791933000#0.226001&2149458937#0.222843&3407262307#0.221283&1420855629#0.221109&3063187629#0.220196&198183680#0.21864&957027974#0.215623&1452054361#0.214754&2690261541#0.214297&2250866761#0.212105&2824591743#0.211998&267943295#0.211077&149516862#0.210342&142686946#0.209584&2776723001#0.209371&2154872721#0.208779&2631306017#0.208037&1493420307#0.207572&3125199256#0.205713&1808124413#0.205318&1560868927#0.203134&1871180522#0.202887&2957192612#0.202649&1275232262#0.20259&332481115#0.202016&2353261026#0.201401&1590644383#0.201119&1686895030#0.200806&3387930846#0.200457&1583354603#0.200083&1062319872#0.199873&793546416#0.19904&1539431280#0.198987&500390459#0.198661&739611941#0.198058&718086371#0.198029&2531769020#0.19791&2225097371#0.197268&2187976325#0.197064&2883355744#0.196308&1125255027#0.196118&933527128#0.195587&1704888835#0.195488&2468316206#0.195198&810722281#0.195105&726297761#0.194686&3044755153#0.194637&1274371380#0.194457&1676110645#0.194118&1596252840#0.193663&2842276134#0.193354&2027183060#0.19285&2947773227#0.192823&2187942341#0.192801&390273715#0.192374&1112012461#0.19178&1362367731#0.190338&1208885641#0.189995&1662459160#0.189844&393824282#0.189424&1222423873#0.188874&1464914160#0.188817&224583846#0.187465&403160447#0.18741&2476803359#0.187201&408305040#0.186992&2776626411#0.18699&1385279026#0.186849&2787034821#0.18644&1640558212#0.186287&2216794920#0.185924&1715121512#0.185837&2009943378#0.185676&1550173310#0.185254&360642023#0.18491&1293202315#0.184392&277314512#0.18302&978534416#0.18283&304571063#0.182666&1173898825#0.182367&2599695023#0.182218&1713546551#0.180747&621270780#0.180588&1435759420#0.180407&1273028372#0.18024&280058393#0.180084&1123583482#0.179734&1635766083#0.179537&1009968841#0.179462&689374724#0.179397&411806600#0.179018&3304192334#0.177888&1154907380#0.177415&2412799820#0.177278&484267201#0.176633&487582915#0.176509&2635044075#0.176479&2968024439#0.176416&1857902680#0.176053&1906208214#0.175404&2778202120#0.17519&610153115#0.175029&264620155#0.174955&427021852#0.174895&2730829420#0.174545&2965457009#0.173748&2033221034#0.172578&2361674714#0.172396&2010117185#0.172346&580620378#0.17232&2378549100#0.171219&2385025733#0.170901&39559110#0.170789&602280478#0.170331&1592544916#0.170247&2216680800#0.169849&1440644182#0.168827&1781198803#0.167736&1150825139#0.167477&2221897360#0.167378&422851302#0.166942&269295969#0.166816&29122044#0.166436&386511760#0.166061&168307770#0.165959&3122444561#0.165731&2482092816#0.16566&2911886881#0.165202&1869068560#0.165142&3477126771#0.165127&2458370426#0.165112&675150242#0.165009&1955198511#0.164927&1505892844#0.163915&1829092107#0.163718&3644362734#0.163548&2008432320#0.163234&2161973383#0.163148&3756123228#0.162967&897592729#0.162614&1512785022#0.162377&859055261#0.162135&2893424563#0.162115&1277165673#0.161753&934629760#0.161448&2325357181#0.161417&50796905#0.161289&1168088799#0.16128&2581754908#0.161211&1278699721#0.159615&909071263#0.159348&1422143823#0.15903&2678581334#0.158697&1635316506#0.15844&932306240#0.158416&1205659880#0.158364&1486454207#0.158341&932171421#0.158071&2567622813#0.158002&1836721421#0.157985&2471563740#0.157906&1468503043#0.157137&2776488701#0.157094&691617403#0.157032&751025908#0.156534&2313444228#0.155218&2554091205#0.155159&3030090721#0.155131&2067856331#0.15493&2706524082#0.154863&376196581#0.154517&1879933744#0.154304&1670167937#0.153577&3044072645#0.153494&1150392315#0.153255&686666525#0.153033&1653706838#0.152649&1107472812#0.152495&98037160#0.152152&841774381#0.152123&2402800521#0.152046&1759815706#0.151958&4015045953#0.151787&2359911280#0.151736&315477299#0.151669&1493246604#0.151459&3041140647#0.151378&1595723416#0.151321&1242380182#0.151011&725768129#0.150846&1174458240#0.150742&1656842681#0.150714&1366720635#0.150502&2998665129#0.150329&558716640#0.149984&2076331378#0.149779&3636057363#0.149748&1250719381#0.149659&95652070#0.149604&582148322#0.149532&2805919982#0.149508&2248247127#0.14908&2753969021#0.148894&334149713#0.148857&386520375#0.148712&1632700212#0.148424&3032095636#0.148058&1441276532#0.147699&2053482333#0.147519&165400375#0.147458&652867720#0.147323&2959015427#0.147143&2901283363#0.147066&1370267845#0.146766&1681234722#0.146536&1578864214#0.146488&1744657463#0.146486&2154652629#0.146462&762938008#0.146127&2877412284#0.14579&1035217580#0.144556&2704323128#0.144451&1315762833#0.144274&2768391330#0.144107&3156712305#0.143491&2018149288#0.143339&47335965#0.14318&3447566742#0.142995&1546882161#0.142931&1153170724#0.142897&1388580333#0.142481&2820186324#0.14197&979116282#0.141879&2370063308#0.141773&1699153164#0.141757&921109197#0.141283&2105515266#0.140982&748596583#0.140718&2036533281#0.140644&1994370062#0.140549&1257534329#0.140452&3032377516#0.140206&3794191720#0.13984&2516880461#0.139681&661475961#0.139679&1308210963#0.139394&2249607918#0.139155&1589541980#0.139124&692811261#0.138959&1690132560#0.138892&2142038501#0.138866&249165245#0.138382&364159203#0.138054&343424080#0.137944&1343630540#0.136961&421719675#0.136818&2491784282#0.136263&229783139#0.136227&1728368703#0.136176&2346468311#0.135817&1172623882#0.135732&158158149#0.135712&334912696#0.135708&1785613260#0.135208&2057129017#0.13496&414819995#0.134361&376522401#0.134271&3030453857#0.134115&786185104#0.133838&322706685#0.133708&1899568363#0.133597&2548265582#0.133441&1744707860#0.133382&2164067029#0.1333&1947486917#0.1332&88590401#0.132695&55249605#0.132596&3416358502#0.132419&1213866742#0.13227&1156532808#0.131922&1038335162#0.131138&873567780#0.130707&3710821041#0.130672&1531169785#0.130551&1844990310#0.130283&3036276314#0.130267&784411820#0.129656&1907479269#0.129611&735766981#0.129408&3630815204#0.129185&2289154709#0.129034&857326567#0.129015&491183599#0.128929&2042400624#0.12888&809834740#0.128837&3362300489#0.128741
	rows := strings.Split(record[*e.Props[0].Index], "&")

	var resultVec []string
	for _, row := range rows {
		cells := strings.Split(row, "#")
		cells[0] = base.TryConvInt64(cells[0])
		resultVec = append(resultVec, fmt.Sprintf(" %s->%s%s:(%s) ", srcVID, cells[0], rank, cells[1]))
	}
	return strings.Join(resultVec, ",")
}

func (e *Edge) maxIndex() int {
	maxIdx := 0
	if e.SrcVID != nil && e.SrcVID.Index != nil && *e.SrcVID.Index > maxIdx {
		maxIdx = *e.SrcVID.Index
	}

	if e.DstVID != nil && e.DstVID.Index != nil && *e.DstVID.Index > maxIdx {
		maxIdx = *e.DstVID.Index
	}

	if e.Rank != nil && e.Rank.Index != nil && *e.Rank.Index > maxIdx {
		maxIdx = *e.Rank.Index
	}

	for _, p := range e.Props {
		if p != nil && p.Index != nil && *p.Index > maxIdx {
			maxIdx = *p.Index
		}
	}

	return maxIdx
}

func combine(cell, val string) string {
	if len(cell) > 0 {
		return fmt.Sprintf("%s/%s", cell, val)
	} else {
		return val
	}
}

func (e *Edge) String() string {
	cells := make([]string, e.maxIndex()+1)
	if e.SrcVID != nil && e.SrcVID.Index != nil {
		cells[*e.SrcVID.Index] = combine(cells[*e.SrcVID.Index], e.SrcVID.String(base.LABEL_SRC_VID))
	}
	if e.DstVID != nil && e.DstVID.Index != nil {
		cells[*e.DstVID.Index] = combine(cells[*e.DstVID.Index], e.DstVID.String(base.LABEL_DST_VID))
	}
	if e.Rank != nil && e.Rank.Index != nil {
		cells[*e.Rank.Index] = combine(cells[*e.Rank.Index], base.LABEL_RANK)
	}
	for _, prop := range e.Props {
		if prop.Index != nil {
			cells[*prop.Index] = combine(cells[*prop.Index], prop.String(*e.Name))
		}
	}
	for i := range cells {
		if cells[i] == "" {
			cells[i] = base.LABEL_IGNORE
		}
	}
	return strings.Join(cells, ",")
}

func (e *Edge) validateAndReset(prefix string) error {
	if e.Name == nil {
		return fmt.Errorf("Please configure edge name in: %s.name", prefix)
	}
	if e.SrcVID != nil {
		if err := e.SrcVID.validateAndReset(fmt.Sprintf("%s.srcVID", prefix), 0); err != nil {
			return err
		}
	} else {
		index := 0
		e.SrcVID = &VID{Index: &index}
	}
	if e.DstVID != nil {
		if err := e.DstVID.validateAndReset(fmt.Sprintf("%s.dstVID", prefix), 1); err != nil {
			return err
		}
	} else {
		index := 1
		e.DstVID = &VID{Index: &index}
	}
	start := 2
	if e.Rank != nil {
		if err := e.Rank.validateAndReset(fmt.Sprintf("%s.rank", prefix), 2); err != nil {
			return err
		}
		start++
	} else {
		if e.WithRanking != nil && *e.WithRanking {
			index := 2
			e.Rank = &Rank{Index: &index}
			start++
		}
	}
	for i := range e.Props {
		if e.Props[i] != nil {
			if err := e.Props[i].validateAndReset(fmt.Sprintf("%s.prop[%d]", prefix, i), i+start); err != nil {
				return err
			}
		} else {
			logger.Errorf("prop %d of edge %s is nil", i, *e.Name)
		}
	}
	return nil
}

func (v *Vertex) FormatValues(record base.Record) string {
	var cells []string
	for _, tag := range v.Tags {
		cells = append(cells, tag.FormatValues(record))
	}
	var vid string
	if v.VID.Function != nil {
		vid = fmt.Sprintf("%s(%q)", *v.VID.Function, record[*v.VID.Index])
	} else {
		vid = base.TryConvInt64(record[*v.VID.Index])
	}
	return fmt.Sprintf(" %s: (%s)", vid, strings.Join(cells, ","))
}

func (v *Vertex) maxIndex() int {
	maxIdx := 0
	if v.VID != nil && v.VID.Index != nil && *v.VID.Index > maxIdx {
		maxIdx = *v.VID.Index
	}
	for _, tag := range v.Tags {
		if tag != nil {
			for _, prop := range tag.Props {
				if prop != nil && prop.Index != nil && *prop.Index > maxIdx {
					maxIdx = *prop.Index
				}
			}
		}
	}

	return maxIdx
}

func (v *Vertex) String() string {
	cells := make([]string, v.maxIndex()+1)
	if v.VID != nil && v.VID.Index != nil {
		cells[*v.VID.Index] = v.VID.String(base.LABEL_VID)
	}
	for _, tag := range v.Tags {
		for _, prop := range tag.Props {
			if prop != nil && prop.Index != nil {
				cells[*prop.Index] = combine(cells[*prop.Index], prop.String(*tag.Name))
			}
		}
	}

	for i := range cells {
		if cells[i] == "" {
			cells[i] = base.LABEL_IGNORE
		}
	}
	return strings.Join(cells, ",")
}

func (v *Vertex) validateAndReset(prefix string) error {
	// if v.Tags == nil {
	// 	return fmt.Errorf("Please configure %.tags", prefix)
	// }
	if v.VID != nil {
		if err := v.VID.validateAndReset(fmt.Sprintf("%s.vid", prefix), 0); err != nil {
			return err
		}
	} else {
		index := 0
		v.VID = &VID{Index: &index}
	}
	j := 1
	for i := range v.Tags {
		if v.Tags[i] != nil {
			if err := v.Tags[i].validateAndReset(fmt.Sprintf("%s.tags[%d]", prefix, i), j); err != nil {
				return err
			}
			j = j + len(v.Tags[i].Props)
		} else {
			logger.Errorf("tag %d is nil", i)
		}
	}
	return nil
}

func (p *Prop) IsStringType() bool {
	return strings.ToLower(*p.Type) == "string"
}

func (p *Prop) IsIntType() bool {
	return strings.ToLower(*p.Type) == "int"
}

func (p *Prop) IsDateTimestampType() bool {
	return strings.HasPrefix(strings.ToLower(*p.Type), "date-timestamp")
}

func (p *Prop) FormatValue(record base.Record) (string, error) {
	if p.Index != nil && *p.Index >= len(record) {
		return "", fmt.Errorf("Prop index %d out range %d of record(%v)", *p.Index, len(record), record)
	}
	r := record[*p.Index]
	if p.IsStringType() {
		return fmt.Sprintf("%q", r), nil
	}
	if p.IsIntType() {
		return base.TryConvInt64(r), nil
	}
	if p.IsDateTimestampType() {
		return base.TryConvDateTimestamp(r, strings.Split(*p.Type, ":")[1]), nil
	}
	return r, nil
}

func (p *Prop) String(prefix string) string {
	return fmt.Sprintf("%s.%s:%s", prefix, *p.Name, *p.Type)
}

func (p *Prop) validateAndReset(prefix string, val int) error {
	*p.Type = strings.ToLower(*p.Type)
	if !base.IsValidType(*p.Type) {
		return fmt.Errorf("Error property type of %s.type: %s", prefix, *p.Type)
	}
	if p.Index == nil {
		p.Index = &val
	} else {
		if *p.Index < 0 {
			logger.Fatalf("Invalid prop index: %d, name: %s, type: %s", *p.Index, *p.Name, *p.Type)
		}
	}
	return nil
}

func (t *Tag) FormatValues(record base.Record) string {
	var cells []string
	for _, p := range t.Props {
		if c, err := p.FormatValue(record); err != nil {
			logger.Fatalf("tag: %v, error: %v", *t, err)
		} else {
			cells = append(cells, c)
		}
	}
	return strings.Join(cells, ",")
}

func (t *Tag) validateAndReset(prefix string, start int) error {
	if t.Name == nil {
		return fmt.Errorf("Please configure the vertex tag name in: %s.name", prefix)
	}

	for i := range t.Props {
		if t.Props[i] != nil {
			if err := t.Props[i].validateAndReset(fmt.Sprintf("%s.props[%d]", prefix, i), i+start); err != nil {
				return err
			}
		} else {
			logger.Errorf("prop %d of tag %s is nil", i, *t.Name)
		}
	}
	return nil
}
