#include <string>
#include <sstream>
#include <fstream>
#include <cstring>
#include <cassert>
#include "mysql_connection.h"

#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>


// g++ -Wall check_ver4.cpp -lmysqlcppconn -std=c++11

char *serIRK, *usrIRK, *pwdIRK, *dbIRK;

using namespace std;
using namespace sql;

void LoadDataConn(const char *_NameTxt) 	/*wczytuje dane logowania do servera z zewnętrznego pliku tekstowego,
						który zawierać powinien 4 linie: nazwa serwera, użytkownik, hasło,
						baza danych.*/
{
    	string line;

    	ifstream data(_NameTxt);
   	if(data.good())
    	{	
        	getline(data, line);
		serIRK= new char[line.length() + 1];
       		strcpy(serIRK, line.c_str());

        	getline(data, line);
		usrIRK= new char[line.length() + 1];
       		strcpy(usrIRK, line.c_str());

        	getline(data, line);
		pwdIRK= new char[line.length() + 1];
       		strcpy(pwdIRK, line.c_str());

        	getline(data, line);
		dbIRK= new char[line.length() + 1];
       		strcpy(dbIRK, line.c_str());
	}
}


class MyException
{
private: 
	string  msg;
public:
	MyException(string str)
	{
		msg = str;
	}
	void printerrmsg()
	{
		cout<<msg<<endl;
	}
};


class BadRecord{

	int pk_;
	string kod_;
	string miejscowosc_;
	string poczta_;
	int err_;
	string remarks_;

	public:

	BadRecord (int pk, string kod, string miejscowosc="brak", string poczta="brak")
		:pk_(pk), kod_(kod), miejscowosc_(miejscowosc), poczta_(poczta){
		if (kod.empty()) err_ = 11;
		else if(kod.size()==5 && isdigit(kod[0]) && isdigit(kod[1]) && isdigit(kod[2]) && isdigit(kod[3]) && isdigit(kod[4])){
			err_ = 0;
			remarks_ = "[brak kreski w kodzie]";
		}
		else if(kod.size()==6 && isdigit(kod[0]) && isdigit(kod[1]) && kod[2]=='-' && isdigit(kod[3]) && isdigit(kod[4]) && isdigit(kod[5])) err_ = 0;
		else err_ = 10;
	}
	
	int Pk()const{
		return pk_;
	}

	string Kod()const{
		return kod_;
	}

	string Poczta()const{
		return poczta_;
	}
	string Miejscowosc()const{
		return miejscowosc_;
	}

	bool IsErr() const{
		return err_!=0;
	}

	string KodDigits() const{
		if(kod_.size() == 6){		
			stringstream ss;
			ss<< kod_[0] << kod_[1] << kod_[3] << kod_[4] << kod_[5];
			return ss.str();
		}
		else return kod_;		
	}
	
	string Error() const{

		switch(err_){
			case 0: return "OK";
			case 10: return "błąd: zły format kodu";
			case 2: return "błąd: kod nie istnieje";
			case 3: return "błąd: do tego kodu nie jest przypisana podana poczta.";
			case 4: return "ostrzeżenie: poczta pasująca do podanego kodu pocztowego jest wpisana w polu miejscowość.";
			case 11: return "błąd: brak wpisanego kodu";
		}
		assert(false);
		return "";
	}

	int NrErr() const{
		return err_;
	}
	
	void InsertError(int er){
		err_ = er;
	}

	void InsertRemark(string remark){
		remarks_ = remarks_.empty() ? remark : remarks_ + " " + remark;
	}

	string Remarks() const{
		return remarks_;
	}
};


ostream& operator << (ostream& wy, BadRecord x){
	wy << x.Pk()<< ";"<< x.Kod() << ";"<< x.Miejscowosc() << ";" << x.Poczta() << ";";

	if(x.IsErr()) wy<<"("  << x.Error() << " [" << x.NrErr() << "]" << ");";
	else wy << "OK;";

	if(!x.Remarks().empty()) wy << x.Remarks();
	return wy;
}

bool SameString (string a, string b){
	transform(a.begin(), a.end(), a.begin(), [](char x){return isalnum(x) ? std::tolower(x) : 0;});
	transform(b.begin(), b.end(), b.begin(), [](char x){return isalnum(x) ? std::tolower(x) : 0;});
	return a==b;
}

class CmyIRK
{
public:
	CmyIRK(const char *_server, const char *_user, const char *_passwd, const char *_db)
	{
		server = _server;
		user   = _user;
		passwd = _passwd;
		db     = _db;

	try 
	{
		driver = get_driver_instance();
		con = NULL;
/* - */
		con = driver -> connect(server, user, passwd);
		con -> setAutoCommit(0);
		con->setSchema(db);
/* - */
	}
	catch (sql::SQLException &e) 
	{
		cerr << "# ERR: SQLException in " << __FILE__;
		cerr << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
		cerr << "# ERR: " << e.what();
		cerr << " (MySQL error code: " << e.getErrorCode();
		cerr << ", SQLState: " << e.getSQLState() << " )" << endl;
		throw MyException("# ERR: Upss ...");
	}
}
~CmyIRK()
{
	if( con!=NULL )
	{
		con -> close();
		delete con;
	}
}

bool CheckPost(BadRecord &record);
void getInfo();

protected:
private:
	string server,user,passwd,db;
	Driver *driver;
	Connection *con;
};

bool CmyIRK::CheckPost(BadRecord &record){
	string remarks="";
	stringstream ss;
        Statement *stmt;
        ResultSet *res;
        ss.clear();
        ss.str("");
        ss << "SELECT * FROM usos_kody_pocztowe u WHERE u.kod=" << record.KodDigits() << ";";
        stmt = con -> createStatement();
        res = stmt -> executeQuery( ss.str().c_str() );

	if(!res->next()){
		record.InsertError(2); 			// brak kodu pocztowego w bazie USOS
		return false;
	}
	else{
		do{
			string tmp=res->getString("poczta");
			if(SameString(tmp,record.Poczta())){
			 record.InsertError(0);			//krotka wpadła do programu, ale wszystko z nią OK
         		 delete res;
         		 delete stmt;
			 return true;
			}
			if(SameString(tmp,record.Miejscowosc())){ 
			 record.InsertError(4); // ostrzeżenie -> poczta pasująca do podanego kodu pocztowego jest wpisana w polu miejscowość
		         delete res;
         		 delete stmt;			
			 return true;
			}
			remarks += 	"/" + tmp + "/";
		}while(res->next());
		record.InsertError(3); //do podanego kodu jest przypisana inna poczta
		record.InsertRemark("[poczty pod tym kodem:"+remarks + "]");
		delete res;
         	delete stmt;
		return false;
	 }
}

void CmyIRK::getInfo(){
         stringstream ss;
	 ofstream data("wyniki");
	 data << "Nr;Kod;Miejscowosc;Poczta;Błędy;Uwagi"<<endl;
         Statement *stmt;
         ResultSet *res;


         ss.clear();
         ss.str("");
         ss << "SELECT * FROM view_irk_osoby i WHERE i.pk NOT IN (SELECT i.pk FROM irk_osoby i, usos_kody_pocztowe u WHERE ((LEFT(i.a_kod,2)=LEFT(u.kod,2) AND RIGHT(i.a_kod,3)=RIGHT(u.kod,3) AND SUBSTRING(i.a_kod,3,1)='-') OR i.a_kod=u.kod) AND (i.a_poczta regexp u.poczta OR u.poczta=i.a_poczta OR (i.a_poczta='' AND (u.poczta=i.a_miejscowosc OR u.poczta regexp i.a_miejscowosc))));";

         stmt = con -> createStatement();
         res = stmt -> executeQuery( ss.str().c_str() );
         while (res->next())
         {
            BadRecord tmp = 
                BadRecord(res->getInt("pk"),res->getString("a_kod"),res->getString("a_miejscowosc"), res->getString("a_poczta"));
		if(!tmp.IsErr()) CheckPost(tmp); //jeśli nie ma jeszcze po konstrukcji obiektu babola to sprawdza czy poczta jest dobra 
		//cout << tmp << endl;
		data << tmp << endl;

	}
        delete res;
        delete stmt;
}

int main()
{
	LoadDataConn("serverData.txt");
	CmyIRK *myIRK;

	try
	{

		myIRK = new CmyIRK(serIRK, usrIRK, pwdIRK, dbIRK);
		myIRK->getInfo();

	}
	catch(MyException& e)
	{
		e.printerrmsg();
	}
	delete myIRK;
	return 0;
}
