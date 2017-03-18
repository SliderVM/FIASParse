package main

import (
	"bytes"
	"database/sql"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/mholt/archiver"
	"github.com/spf13/viper"
	pb "gopkg.in/cheggaaa/pb.v1"
	"gopkg.in/doug-martin/goqu.v3"
	_ "gopkg.in/doug-martin/goqu.v3/adapters/postgres"
)

var dirName string

var ACTSTAT_PATTERN = regexp.MustCompile("^(AS_ACTSTAT_)[0-9]{8}_.+")
var ADDROBJ_PATTERN = regexp.MustCompile("^(AS_ADDROBJ_)[0-9]{8}_.+")
var CENTERST_PATTERN = regexp.MustCompile("^(AS_CENTERST_)[0-9]{8}_.+")
var CURENTST_PATTERN = regexp.MustCompile("^(AS_CURENTST_)[0-9]{8}_.+")
var DEL_ADDROBJ_PATTERN = regexp.MustCompile("^(AS_DEL_ADDROBJ_)[0-9]{8}_.+")
var DEL_HOUSE_PATTERN = regexp.MustCompile("^(AS_DEL_HOUSE_)[0-9]{8}_.+")
var DEL_HOUSEINT_PATTERN = regexp.MustCompile("^(AS_DEL_HOUSEINT_)[0-9]{8}_.+")
var DEL_NORMDOC_PATTERN = regexp.MustCompile("^(AS_DEL_NORMDOC_)[0-9]{8}_.+")
var ESTSTAT_PATTERN = regexp.MustCompile("^(AS_ESTSTAT_)[0-9]{8}_.+")
var HOUSE_PATTERN = regexp.MustCompile("^(AS_HOUSE_)[0-9]{8}_.+")
var HOUSEINT_PATTERN = regexp.MustCompile("^(AS_HOUSEINT_)[0-9]{8}_.+")
var HSTSTAT_PATTERN = regexp.MustCompile("^(AS_HSTSTAT_)[0-9]{8}_.+")
var INTVSTAT_PATTERN = regexp.MustCompile("^(AS_INTVSTAT_)[0-9]{8}_.+")
var LANDMARK_PATTERN = regexp.MustCompile("^(AS_LANDMARK_)[0-9]{8}_.+")
var NDOCTYPE_PATTERN = regexp.MustCompile("^(AS_NDOCTYPE_)[0-9]{8}_.+")
var NORMDOC_PATTERN = regexp.MustCompile("^(AS_NORMDOC_)[0-9]{8}_.+")
var OPERSTAT_PATTERN = regexp.MustCompile("^(AS_OPERSTAT_)[0-9]{8}_.+")
var SOCRBASE_PATTERN = regexp.MustCompile("^(AS_SOCRBASE_)[0-9]{8}_.+")
var STRSTAT_PATTERN = regexp.MustCompile("^(AS_STRSTAT_)[0-9]{8}_.+")
var STEAD_PATTERN = regexp.MustCompile("^(AS_STEAD_)[0-9]{8}_.+")
var ROOM_PATTERN = regexp.MustCompile("^(AS_ROOM_)[0-9]{8}_.+")

// PassThru - структура для вывода кол-ва считанных байт
type PassThru struct {
	io.Reader
	total int64 // Total # of bytes transferred
}

// MyRespEnvelope - запрос новых файлов
type MyRespEnvelope struct {
	XMLName xml.Name
	Body    Body
}

// Body - тело ответа запроса новых файлов
type Body struct {
	XMLName     xml.Name
	GetResponse GetLastDownloadFileInfoResponse `xml:"GetLastDownloadFileInfoResponse"`
}

// GetLastDownloadFileInfoResponse - структрура запроса новых файлов
type GetLastDownloadFileInfoResponse struct {
	XMLName                       xml.Name                      `xml:"GetLastDownloadFileInfoResponse"`
	GetLastDownloadFileInfoResult GetLastDownloadFileInfoResult `xml:"GetLastDownloadFileInfoResult"`
}

// GetLastDownloadFileInfoResult - структрура ответа на запрос новых файлов
type GetLastDownloadFileInfoResult struct {
	XMLName            xml.Name `xml:"GetLastDownloadFileInfoResult"`
	VersionId          int      `xml:"VersionId"`
	TextVersion        string   `xml:"TextVersion"`
	FiasCompleteDbfUrl string   `xml:"FiasCompleteDbfUrl"`
	FiasCompleteXmlUrl string   `xml:"FiasCompleteXmlUrl"`
	FiasDeltaDbfUrl    string   `xml:"FiasDeltaDbfUrl"`
	FiasDeltaXmlUrl    string   `xml:"FiasDeltaXmlUrl"`
	Kladr4ArjUrl       string   `xml:"Kladr4ArjUrl"`
	Kladr47ZUrl        string   `xml:"Kladr47ZUrl"`
}

// ActualStatus - Статус актуальности ФИАС
type ActualStatus struct {
	ACTSTATID int    `xml:"ACTSTATID,attr"`
	NAME      string `xml:"NAME,attr"`
}

// Object - Классификатор адресообразующих элементов
type Object struct {
	AOGUID     string `xml:"AOGUID,attr"`
	FORMALNAME string `xml:"FORMALNAME,attr"`
	REGIONCODE string `xml:"REGIONCODE,attr"`
	AUTOCODE   string `xml:"AUTOCODE,attr"`
	AREACODE   string `xml:"AREACODE,attr"`
	CITYCODE   string `xml:"CITYCODE,attr"`
	CTARCODE   string `xml:"CTARCODE,attr"`
	PLACECODE  string `xml:"PLACECODE,attr"`
	STREETCODE string `xml:"STREETCODE,attr"`
	EXTRCODE   string `xml:"EXTRCODE,attr"`
	SEXTCODE   string `xml:"SEXTCODE,attr"`
	OFFNAME    string `xml:"OFFNAME,attr"`
	POSTALCODE string `xml:"POSTALCODE,attr"`
	IFNSFL     string `xml:"IFNSFL,attr"`
	TERRIFNSFL string `xml:"TERRIFNSFL,attr"`
	IFNSUL     string `xml:"IFNSUL,attr"`
	TERRIFNSUL string `xml:"TERRIFNSUL,attr"`
	OKATO      string `xml:"OKATO,attr"`
	OKTMO      string `xml:"OKTMO,attr"`
	UPDATEDATE string `xml:"UPDATEDATE,attr"`
	SHORTNAME  string `xml:"SHORTNAME,attr"`
	AOLEVEL    int    `xml:"AOLEVEL,attr"`
	PARENTGUID string `xml:"PARENTGUID,attr"`
	AOID       string `xml:"AOID,attr"`
	PREVID     string `xml:"PREVID,attr"`
	NEXTID     string `xml:"NEXTID,attr"`
	CODE       string `xml:"CODE,attr"`
	PLAINCODE  string `xml:"PLAINCODE,attr"`
	ACTSTATUS  int    `xml:"ACTSTATUS,attr"`
	CENTSTATUS int    `xml:"CENTSTATUS,attr"`
	OPERSTATUS int    `xml:"OPERSTATUS,attr"`
	CURRSTATUS int    `xml:"CURRSTATUS,attr"`
	STARTDATE  string `xml:"STARTDATE,attr"`
	ENDDATE    string `xml:"ENDDATE,attr"`
	NORMDOC    string `xml:"NORMDOC,attr"`
	LIVESTATUS byte   `xml:"LIVESTATUS,attr"`
	CADNUM     string `xml:"CADNUM,attr"`
	DIVTYPE    int    `xml:"DIVTYPE,attr"`
}

// CenterStatus - Статус центра
type CenterStatus struct {
	CENTERSTID int    `xml:"CENTERSTID,attr"`
	NAME       string `xml:"NAME,attr"`
}

// CurrentStatus - Статус актуальности КЛАДР 4.0
type CurrentStatus struct {
	CURENTSTID int    `xml:"CURENTSTID,attr"`
	NAME       string `xml:"NAME,attr"`
}

// EstateStatus - Признак владения
type EstateStatus struct {
	ESTSTATID int    `xml:"ESTSTATID,attr"`
	NAME      string `xml:"NAME,attr"`
	SHORTNAME string `xml:"SHORTNAME,attr"`
}

// House - Сведения по номерам домов улиц городов и населенных пунктов
type House struct {
	PostalCode string `xml:"POSTALCODE,attr"`
	RegionCode string `xml:"REGIONCODE,attr"`
	IFNSFL     string `xml:"IFNSFL,attr"`
	TerrIFNSFL string `xml:"TERRIFNSFL,attr"`
	IFNSUL     string `xml:"IFNSUL,attr"`
	TerrIFNSUL string `xml:"TERRIFNSUL,attr"`
	OKATO      string `xml:"OKATO,attr"`
	OKTMO      string `xml:"OKTMO,attr"`
	UPDATEDATE string `xml:"UPDATEDATE,attr"`
	HouseNum   string `xml:"HOUSENUM,attr"`
	ESTStatus  int    `xml:"ESTSTATUS,attr"`
	//ESTStatus string `xml:"ESTSTATUS,attr"`
	BuildNum  string `xml:"BUILDNUM,attr"`
	StrucNum  string `xml:"STRUCNUM,attr"`
	STRStatus int    `xml:"STRSTATUS,attr"`
	//STRStatus string `xml:"STRSTATUS,attr"`
	HouseID    string `xml:"HOUSEID,attr"`
	HouseGUID  string `xml:"HOUSEGUID,attr"`
	AOGUID     string `xml:"AOGUID,attr"`
	STARTDATE  string `xml:"STARTDATE,attr"`
	ENDDATE    string `xml:"ENDDATE,attr"`
	StatStatus int    `xml:"STATSTATUS,attr"`
	//StatStatus string `xml:"STATSTATUS,attr"`
	NormDoc string `xml:"NORMDOC,attr"`
	Counter int    `xml:"COUNTER,attr"`
	//Counter string `xml:"COUNTER,attr"`
	CadNum  string `xml:"CADNUM,attr"`
	DviType int    `xml:"DVITYPE,attr"`
	//DviType string `xml:"DVITYPE,attr"`
}

// HouseInterval - Интервалы домов
type HouseInterval struct {
	POSTALCODE string `xml:"POSTALCODE,attr"`
	IFNSFL     string `xml:"IFNSFL,attr"`
	TERRIFNSFL string `xml:"TERRIFNSFL,attr"`
	IFNSUL     string `xml:"IFNSUL,attr"`
	TERRIFNSUL string `xml:"TERRIFNSUL,attr"`
	OKATO      string `xml:"OKATO,attr"`
	OKTMO      string `xml:"OKTMO,attr"`
	UPDATEDATE string `xml:"UPDATEDATE,attr"`
	INTSTART   int    `xml:"INTSTART,attr"`
	INTEND     int    `xml:"INTEND,attr"`
	HOUSEINTID string `xml:"HOUSEINTID,attr"`
	INTGUID    string `xml:"INTGUID,attr"`
	AOGUID     string `xml:"AOGUID,attr"`
	STARTDATE  string `xml:"STARTDATE,attr"`
	ENDDATE    string `xml:"ENDDATE,attr"`
	INTSTATUS  int    `xml:"INTSTATUS,attr"`
	NORMDOC    string `xml:"NORMDOC,attr"`
	COUNTER    int    `xml:"COUNTER,attr"`
}

// HouseStateStatus - Статус состояния домов
type HouseStateStatus struct {
	HOUSESTID int    `xml:"HOUSESTID,attr"`
	NAME      string `xml:"NAME,attr"`
}

// IntervalStatus - Статус интервала домов
type IntervalStatus struct {
	INTVSTATID int    `xml:"INTVSTATID,attr"`
	NAME       string `xml:"NAME,attr"`
}

// Landmark - Описание мест расположения  имущественных объектов
type Landmark struct {
	LOCATION   string `xml:"LOCATION,attr"`
	REGIONCODE string `xml:"REGIONCODE,attr"`
	POSTALCODE string `xml:"POSTALCODE,attr"`
	IFNSFL     string `xml:"IFNSFL,attr"`
	TERRIFNSFL string `xml:"TERRIFNSFL,attr"`
	IFNSUL     string `xml:"IFNSUL,attr"`
	TERRIFNSUL string `xml:"TERRIFNSUL,attr"`
	OKATO      string `xml:"OKATO,attr"`
	OKTMO      string `xml:"OKTMO,attr"`
	UPDATEDATE string `xml:"UPDATEDATE,attr"`
	LANDID     string `xml:"LANDID,attr"`
	LANDGUID   string `xml:"LANDGUID,attr"`
	AOGUID     string `xml:"AOGUID,attr"`
	STARTDATE  string `xml:"STARTDATE,attr"`
	ENDDATE    string `xml:"ENDDATE,attr"`
	NORMDOC    string `xml:"NORMDOC,attr"`
	CADNUM     string `xml:"CADNUM,attr"`
}

// NormativeDocumentType - Тип нормативного документа
type NormativeDocumentType struct {
	NDTYPEID int    `xml:"NDTYPEID,attr"`
	NAME     string `xml:"NAME,attr"`
}

// NormativeDocument - Сведения по нормативному документу, являющемуся основанием присвоения адресному элементу наименования
type NormativeDocument struct {
	NORMDOCID string `xml:"NORMDOCID,attr"`
	DOCNAME   string `xml:"DOCNAME,attr"`
	DOCDATE   string `xml:"DOCDATE,attr"`
	DOCNUM    string `xml:"DOCNUM,attr"`
	DOCTYPE   int    `xml:"DOCTYPE,attr"`
	DOCIMGID  int    `xml:"DOCIMGID,attr"`
}

// OperationStatus - Статус действия
type OperationStatus struct {
	OPERSTATID int    `xml:"OPERSTATID,attr"`
	NAME       string `xml:"NAME,attr"`
}

// Room - Классификатор помещениях
type Room struct {
	RoomGuid   string `xml:"ROOMGUID,attr"`
	FlatNumber string `xml:"FLATNUMBER,attr"`
	FlatType   int    `xml:"FLATTYPE,attr"`
	RoomNumber string `xml:"ROOMNUMBER,attr"`
	RoomType   int    `xml:"ROOMTYPE,attr"`
	RegionCode string `xml:"REGIONCODE,attr"`
	PostalCode string `xml:"POSTALCODE,attr"`
	UpdateDate string `xml:"UPDATEDATE,attr"`
	HouseGuid  string `xml:"HOUSEGUID,attr"`
	RoomId     string `xml:"ROOMID,attr"`
	PrevId     string `xml:"PREVID,attr"`
	NextId     string `xml:"NEXTID,attr"`
	StartDate  string `xml:"STARTDATE,attr"`
	EndDate    string `xml:"ENDDATE,attr"`
	LiveStatus string `xml:"LIVESTATUS,attr"`
	NormDoc    string `xml:"NORMDOC,attr"`
	OperStatus int    `xml:"OPERSTATUS,attr"`
	CadNum     string `xml:"CADNUM,attr"`
	RoomCadNum string `xml:"ROOMCADNUM,attr"`
}

// AddressObjectType - Тип адресного объекта
type AddressObjectType struct {
	LEVEL    int    `xml:"LEVEL,attr"`
	SCNAME   string `xml:"SCNAME,attr"`
	SOCRNAME string `xml:"SOCRNAME,attr"`
	KODTST   string `xml:"KOD_T_ST,attr"`
}

// Stead - Классификатор земельных участков
type Stead struct {
	STEADGUID  string `xml:"STEADGUID,attr"`
	NUMBER     string `xml:"NUMBER,attr"`
	REGIONCODE string `xml:"REGIONCODE,attr"`
	POSTALCODE string `xml:"POSTALCODE,attr"`
	IFNSFL     string `xml:"IFNSFL,attr"`
	TERRIFNSFL string `xml:"TERRIFNSFL,attr"`
	IFNSUL     string `xml:"IFNSUL,attr"`
	TERRIFNSUL string `xml:"TERRIFNSUL,attr"`
	OKATO      string `xml:"OKATO,attr"`
	OKTMO      string `xml:"OKTMO,attr"`
	UPDATEDATE string `xml:"UPDATEDATE,attr"`
	PARENTGUID string `xml:"PARENTGUID,attr"`
	STEADID    string `xml:"STEADID,attr"`
	PREVID     string `xml:"PREVID,attr"`
	NEXTID     string `xml:"NEXTID,attr"`
	OPERSTATUS int    `xml:"OPERSTATUS,attr"`
	STARTDATE  string `xml:"STARTDATE,attr"`
	ENDDATE    string `xml:"ENDDATE,attr"`
	NORMDOC    string `xml:"NORMDOC,attr"`
	LIVESTATUS byte   `xml:"LIVESTATUS,attr"`
	CADNUM     string `xml:"CADNUM,attr"`
	DIVTYPE    int    `xml:"DIVTYPE,attr"`
}

// StructureStatus - Признак строения
type StructureStatus struct {
	STRSTATID int    `xml:"STRSTATID,attr"`
	NAME      string `xml:"NAME,attr"`
	SHORTNAME string `xml:"SHORTNAME,attr"`
}

func (pt *PassThru) Read(p []byte) (int, error) {
	n, err := pt.Reader.Read(p)
	pt.total += int64(n)

	if err == nil {
		fmt.Println("Read", n, "bytes for a total of", pt.total)
	}

	return n, err
}

func checkNewFile(connectionString string) (path string, err error) {
	url := "http://fias.nalog.ru/WebServices/Public/DownloadService.asmx"
	var jsonStr = []byte(`<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:dow="http://fias.nalog.ru/WebServices/Public/DownloadService.asmx">
   <soap:Header/>
   <soap:Body>
      <dow:GetLastDownloadFileInfo/>
   </soap:Body>
</soap:Envelope>`)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
	req.Header.Set("Content-Type", "application/soap+xml; charset=utf-8")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
		return "", err
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	ver := &MyRespEnvelope{}
	xml.Unmarshal(body, &ver)
	versionID := ver.Body.GetResponse.GetLastDownloadFileInfoResult.VersionId

	pgDb, err := sql.Open("postgres", connectionString)
	if err != nil {
		log.Printf("\nОшибка %s при открытие БД", err)
		return "", err
	}
	defer pgDb.Close()

	gq := goqu.New("postgres", pgDb)
	query, _, _ := gq.From("config").Select("value").Where(goqu.Ex{
		"id": "TextVersion",
	}).ToSql()

	var fileVersion string
	err = pgDb.QueryRow(query).Scan(&fileVersion)
	switch {
	case err == sql.ErrNoRows:
		log.Printf("No TextVersion in config.")
		return "", err
	case err != nil:
		log.Fatal(err)
		return "", err
	default:
	}

	if fileVersion != string(versionID) {
		update := gq.From("config").
			Where(goqu.I("id").Eq("TextVersion")).
			Update(goqu.Record{"value": string(versionID)})

		if _, err := update.Exec(); err != nil {
			log.Printf("\nОшибка %s при записи версии файла", err.Error())
			return "", err
		}

		return ver.Body.GetResponse.GetLastDownloadFileInfoResult.FiasCompleteXmlUrl, err
	}

	return "", err
}

// DownLoadFile - Грузим файл из ФИАС
func DownLoadFile(path string) (fileName string, err error) {
	fileName = "fias.rar"

	if _, err := os.Stat(fileName); !os.IsNotExist(err) {
		os.RemoveAll(dirName)
		os.Remove(fileName)
	}

	output, err := os.Create(fileName)
	if err != nil {
		fmt.Println("Error while creating", "file", "-", err)
		return
	}
	defer output.Close()

	response, err := http.Get(path)
	if err != nil {
		fmt.Println("Error while downloading", path, "-", err)
		return
	}
	defer response.Body.Close()

	fileSize := int(response.ContentLength)

	bar := pb.New(fileSize).SetUnits(pb.U_BYTES)
	bar.Start()
	reader := bar.NewProxyReader(response.Body)

	n, err := io.Copy(output, reader)
	if err != nil {
		fmt.Println("Error while downloading", path, "-", err)
		return
	}

	fmt.Println(n, "bytes downloaded.")

	return fileName, err
}

// UnRar - распаковываем архив
func UnRar(fileName string) error {
	err := archiver.Rar.Open(fileName, "FIAS")
	return err
}

// Parse - парсер файлов
func Parse(f string, table string, connectionString string, elementName string, r interface{}) error {
	fmt.Printf("Открываем файл %s\n", f)

	file, err := os.Open(f)
	if err != nil {
		fmt.Printf("\nОшибка %s открытия файла", err)
		return err
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		fmt.Printf("\nОшибка %s при получении размера файла", err)
		return err
	}

	pgDb, err := sql.Open("postgres", connectionString)
	if err != nil {
		fmt.Printf("\nОшибка %s при открытие БД", err)
		return err
	}
	defer pgDb.Close()

	gq := goqu.New("postgres", pgDb)

	tempTableName := table
	if fi.Size() > int64(20388921) {
		tempTableName = "temp_" + table
		fmt.Printf("\nБольшой файл, создаем временную таблицу %s", tempTableName)
		createTemplateTable := fmt.Sprintf("CREATE TABLE %s ( like %s including all);", tempTableName, table)

		_, err = pgDb.Exec(createTemplateTable)
		if err != nil {
			fmt.Printf("Ошибка %s при создании временной таблицы", err)
			return err
		}
		defer pgDb.Exec("DROP TABLE " + tempTableName + ";")
	}

	if tempTableName == table {
		result, err := pgDb.Exec("TRUNCATE " + tempTableName + ";")
		if err != nil {
			fmt.Printf("\nОшибка %s при удалении данных из таблице", err)
			return err
		}
		rowsAffected, _ := result.RowsAffected()
		fmt.Printf("\nУдалено %v из таблицы %s", rowsAffected, tempTableName)
	}

	s := reflect.ValueOf(r).Elem()
	columnsCount := s.NumField()
	columnsName := make([]string, columnsCount)

	fmt.Printf("\nВсего строк - %v \n", columnsCount)
	for ii := 0; ii < columnsCount; ii++ {
		columnsName[ii] = strings.ToLower(s.Type().Field(ii).Name)
	}

	var inElement string
	bar := pb.New(int(fi.Size())).SetUnits(pb.U_BYTES)
	bar.Start()
	reader := bar.NewProxyReader(file)
	decoder := xml.NewDecoder(reader)
	arguments := []goqu.Record{}
	var total int64
	for {
		// Read tokens from the XML document in a stream.
		t, _ := decoder.Token()
		if t == nil {
			break
		}

		// Inspect the type of the token just read.
		switch se := t.(type) {
		case xml.StartElement:
			// If we just read a StartElement token
			inElement = se.Name.Local
			// ...and its name is "page"
			if inElement == elementName {
				err := decoder.DecodeElement(&r, &se)
				if err != nil {
					fmt.Printf("\nОшибка при декодинге %s ", err)
					return err
				}
				argument := make(goqu.Record)
				for j := 0; j < columnsCount; j++ {
					f := s.Field(j)
					argument[columnsName[j]] = f.Interface()
				}
				arguments = append(arguments, argument)
				total++
			}

		default:
		}

		if len(arguments) == 5000 {
			if _, err := gq.From(tempTableName).Insert(arguments).Exec(); err != nil {
				fmt.Println(err.Error())
				return err
			}
			arguments = []goqu.Record{}
		}
	}

	if len(arguments) > 0 {
		if _, err := gq.From(tempTableName).Insert(arguments).Exec(); err != nil {
			fmt.Println(err.Error())
			return err
		}

		if tempTableName != table {
			fmt.Printf("\nНачинаем переносить данные")
			result, err := pgDb.Exec("DROP TABLE " + table + "; ALTER TABLE " + tempTableName + " RENAME TO " + table + ";")
			if err != nil {
				fmt.Println(err)
				return err
			}

			rowsAffected, _ := result.RowsAffected()
			fmt.Printf("\nТаблица скопирована, затронуто %v строк\n", rowsAffected)
		}
		arguments = []goqu.Record{}
	}

	fmt.Println()

	return err
}

func main() {
	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	err := viper.ReadInConfig()
	if err != nil {
		log.Printf("Ошибка %s чтения конфига", err)
	} else {
		server := viper.GetString("datebase.server")
		port := viper.GetInt("datebase.port")
		user := viper.GetString("datebase.user")
		password := viper.GetString("datebase.password")
		base := viper.GetString("datebase.base")
		dirName = viper.GetString("datebase.dir_name")
		dbinfo := fmt.Sprintf("host=%s port=%v user=%s password=%s dbname=%s sslmode=disable application_name='FIAS Parser'",
			server, port, user, password, base)

		fmt.Println(dbinfo)
		for {

			check, err := checkNewFile(dbinfo)
			if err != nil {
				log.Println(err)
				break
			}
			fmt.Println(check)

			if check == "" {
				log.Println("Нет новых файлов")
				time.Sleep(24 * time.Hour)
				break
			}

			fileName, err := DownLoadFile(check)

			if err != nil {
				log.Printf("Ошибка %s при загрузке файла", err)
				break
			}

			err = UnRar(fileName)
			if err != nil {
				log.Printf("Ошибка %s при распаковки файла", err)
				break
			}

			dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
			if err != nil {
				log.Fatal(err)
			}
			dir += dirName

			files, err := ioutil.ReadDir("FIAS")
			if err != nil {
				log.Fatal(err)
			}

			for _, file := range files {
				switch {
				case ACTSTAT_PATTERN.MatchString(file.Name()):
					fmt.Println("ACTSTAT_PATTERN")
					r := new(ActualStatus)
					Parse(dir+file.Name(), "actual_status", dbinfo, "ActualStatus", r)
				case ADDROBJ_PATTERN.MatchString(file.Name()):
					fmt.Println("ADDROBJ_PATTERN")
					r := new(Object)
					Parse(dir+file.Name(), "address_objects", dbinfo, "Object", r)
				case CENTERST_PATTERN.MatchString(file.Name()):
					fmt.Println("ADDROBJ_PATTERN")
					r := new(CenterStatus)
					Parse(dir+file.Name(), "center_status", dbinfo, "CenterStatus", r)
				case CURENTST_PATTERN.MatchString(file.Name()):
					fmt.Println("CURENTST_PATTERN")
					r := new(CenterStatus)
					Parse(dir+file.Name(), "current_status", dbinfo, "CurrentStatus", r)
				case DEL_ADDROBJ_PATTERN.MatchString(file.Name()):
					fmt.Println("DEL_ADDROBJ_PATTERN")
					r := new(Object)
					Parse(dir+file.Name(), "del_address_objects", dbinfo, "Object", r)
				case DEL_HOUSE_PATTERN.MatchString(file.Name()):
					fmt.Println("DEL_HOUSE_PATTERN")
					r := new(House)
					Parse(dir+file.Name(), "del_house", dbinfo, "House", r)
				case DEL_HOUSEINT_PATTERN.MatchString(file.Name()):
					fmt.Println("DEL_HOUSEINT_PATTERN")
					r := new(HouseInterval)
					Parse(dir+file.Name(), "del_house_interval", dbinfo, "HouseInterval", r)
				case DEL_NORMDOC_PATTERN.MatchString(file.Name()):
					fmt.Println("DEL_NORMDOC_PATTERN")
					r := new(NormativeDocument)
					Parse(dir+file.Name(), "del_normative_document", dbinfo, "NormativeDocument", r)
				case ESTSTAT_PATTERN.MatchString(file.Name()):
					fmt.Println("ESTSTAT_PATTERN")
					r := new(EstateStatus)
					Parse(dir+file.Name(), "estate_status", dbinfo, "EstateStatus", r)
				case HOUSE_PATTERN.MatchString(file.Name()):
					fmt.Println("HOUSEINT_PATTERN")
					r := new(House)
					Parse(dir+file.Name(), "house", dbinfo, "House", r)
				case HOUSEINT_PATTERN.MatchString(file.Name()):
					fmt.Println("HOUSEINT_PATTERN")
					r := new(HouseInterval)
					Parse(dir+file.Name(), "house_interval", dbinfo, "HouseInterval", r)
				case HSTSTAT_PATTERN.MatchString(file.Name()):
					fmt.Println("HSTSTAT_PATTERN")
					r := new(HouseStateStatus)
					Parse(dir+file.Name(), "house_state_status", dbinfo, "HouseStateStatus", r)
				case INTVSTAT_PATTERN.MatchString(file.Name()):
					fmt.Println("INTVSTAT_PATTERN")
					r := new(IntervalStatus)
					Parse(dir+file.Name(), "interval_status", dbinfo, "IntervalStatus", r)
				case LANDMARK_PATTERN.MatchString(file.Name()):
					fmt.Println("LANDMARK_PATTERN")
					r := new(Landmark)
					Parse(dir+file.Name(), "landmark", dbinfo, "Landmark", r)
				case NDOCTYPE_PATTERN.MatchString(file.Name()):
					fmt.Println("NDOCTYPE_PATTERN")
					r := new(NormativeDocumentType)
					Parse(dir+file.Name(), "normative_document_type", dbinfo, "NormativeDocumentType", r)
				case NORMDOC_PATTERN.MatchString(file.Name()):
					fmt.Println("NORMDOC_PATTERN")
					r := new(NormativeDocumentType)
					Parse(dir+file.Name(), "normative_document", dbinfo, "NormativeDocument", r)
				case OPERSTAT_PATTERN.MatchString(file.Name()):
					fmt.Println("OPERSTAT_PATTERN")
					r := new(OperationStatus)
					Parse(dir+file.Name(), "operation_status", dbinfo, "OperationStatus", r)
				case SOCRBASE_PATTERN.MatchString(file.Name()):
					fmt.Println("SOCRBASE_PATTERN")
					r := new(AddressObjectType)
					Parse(dir+file.Name(), "address_object_type", dbinfo, "AddressObjectType", r)
				case STRSTAT_PATTERN.MatchString(file.Name()):
					fmt.Println("STRSTAT_PATTERN")
					r := new(StructureStatus)
					Parse(dir+file.Name(), "structure_status", dbinfo, "StructureStatus", r)
				case STEAD_PATTERN.MatchString(file.Name()):
					fmt.Println("STEAD_PATTERN")
					r := new(Stead)
					Parse(dir+file.Name(), "steads", dbinfo, "Stead", r)
				case ROOM_PATTERN.MatchString(file.Name()):
					fmt.Println("ROOM_PATTERN")
					r := new(Room)
					Parse(dir+file.Name(), "rooms", dbinfo, "Room", r)
				default:
					fmt.Println("It doesn't match")
				}
			}

			time.Sleep(150 * time.Hour)
		}
	}
}
