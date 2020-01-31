import 'package:flutter/material.dart';
import 'package:flutter/cupertino.dart';
import 'package:flutter/services.dart';
import 'package:flutter/widgets.dart';
import 'LoginPage.dart';

List<String> courses = new List();
void main() => runApp(MyApp());

class MyApp extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
        return new MaterialApp(
          title: 'MSA Recommender',
          //theme: theme,
          home: LoadPage(),
        );
  }
}
class LoadPage extends StatefulWidget {
  LoadPage({Key key}) : super(key: key);

  @override
  LoadPageState createState() => LoadPageState();
}

class LoadPageState extends State<LoadPage> {
  final sid = TextEditingController();
  final pass = TextEditingController();
  final passRegex = r"^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)[a-zA-Z\d]{8,}$";
  final sidRegex  = r"[0-9]+";
  @override
  void dispose() {
    sid.dispose();
    pass.dispose();
    super.dispose();
  }
  @override
  Widget build(BuildContext context) {
    return Scaffold(
      backgroundColor: Color(0xff34495e),
      body: SafeArea(
        child: Center(
          child: Column(
              mainAxisSize: MainAxisSize.min,
              children: <Widget>[
                new Expanded(
                  child: new Image.asset("assets/images/msa_logo.jpeg"),
                ),
                new Card(
                  margin: EdgeInsets.fromLTRB(25, 0, 25, 5),
                  child: new Row(
                    mainAxisSize: MainAxisSize.min,
                    children: <Widget>[
                      new Expanded(
                        child: new TextField(
                          controller: sid,
                          keyboardType: TextInputType.number,
                          inputFormatters: <TextInputFormatter>
                          [WhitelistingTextInputFormatter.digitsOnly],
                          decoration: const InputDecoration(
                            labelText: 'Student ID',
                            icon: Icon(Icons.person),
                          ),
                        ),
                      ),
                    ],
                  ),
                ),
                new Card(
                  margin: EdgeInsets.fromLTRB(25, 5, 25, 0),
                  child: new Row(
                    children: <Widget>[
                      new Expanded(
                         child: new TextField(
                          obscureText: true,
                          controller: pass,
                          decoration: const InputDecoration(
                            labelText: 'Password',
                            icon: Icon(Icons.lock),
                          ),
                        ),
                      )
                    ],
                  ),
                ),
                new Expanded(
                  child: new CupertinoButton(
                    child: new Text(
                      'Login',
                        style: new TextStyle(
                        color: Color(0xffedc50b),
                        fontWeight: FontWeight.bold,
                        fontFamily: 'Fondamento',
                        fontSize: 32,
                      ),
                    ),
                    onPressed: () {
                      if (sid.text.isNotEmpty && pass.text.isNotEmpty &&
                          matchesRegex(sidRegex,sid.text.toString()) &&
                          matchesRegex(passRegex, pass.text.toString())) {
                          Navigator.push(context,
                              MaterialPageRoute(builder: (context) => Login()));
                      }
                    },
                  ),
                ), //login
              ]),
        ),
      ), 
    );
  }
  bool matchesRegex(String regex, String string)
  {
    RegExp exp = new RegExp(regex);
    return exp.hasMatch(string);
  }
}
