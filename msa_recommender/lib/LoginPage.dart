import 'package:flutter/material.dart';
import 'package:flutter/cupertino.dart';
import 'package:flutter/widgets.dart';
import 'AddCoursesPage.dart';

class Login extends StatefulWidget {
  Login({Key key}) : super(key: key);

  @override
  LoginPage createState() => LoginPage();
}

class LoginPage extends State<Login> {
  final _sid = TextEditingController(),
      _gpa = TextEditingController();

  @override
  void dispose() {
    _sid.dispose();
    _gpa.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
        appBar: CupertinoNavigationBar(
          middle: Text(
            'Login Page',
            style: TextStyle(color: Colors.teal),
          ),
        ),
        body: SafeArea(
            child: ListView(children: <Widget>[
              Column(
                crossAxisAlignment: CrossAxisAlignment.center,
                mainAxisAlignment: MainAxisAlignment.center,
                children: <Widget>[
                  new Padding(
                    padding: EdgeInsets.fromLTRB(0, 20, 0, 0),
                    child: Text(
                      "Fill Information As Required",
                      style: TextStyle(
                        fontSize: 20,
                        color: Colors.teal,
                      ),
                    ),
                  ),
                  new Padding(
                    padding: EdgeInsets.fromLTRB(25, 50, 25, 0),
                    child: CupertinoTextField(
                      padding: EdgeInsets.fromLTRB(10, 5, 10, 5),
                      style: TextStyle(
                          fontSize: 16,
                          color: Theme.of(context).brightness == Brightness.dark ?
                          Colors.white : Colors.black
                      ),
                      placeholder: "Student ID",
                      controller: _sid,
                      keyboardType: TextInputType.number,
                      prefix: Icon(
                        Icons.person,
                        color: Colors.deepPurple,
                      ),
                    ),
                  ),
                  new Padding(
                    padding: EdgeInsets.fromLTRB(25, 20, 25, 0),
                    child: CupertinoTextField(
                      padding: EdgeInsets.fromLTRB(10, 5, 10, 5),
                      style: TextStyle(
                          fontSize: 16,
                          color: Theme.of(context).brightness == Brightness.dark ?
                          Colors.white : Colors.black
                      ),
                      placeholder: "GPA",
                      controller: _gpa,
                      keyboardType: TextInputType.numberWithOptions(
                          decimal: true,
                          signed: false,
                      ),
                      prefix: Icon(
                        Icons.school,
                        color: Colors.deepPurple,
                      ),
                    ),
                  ),
                  new Padding(
                    padding: EdgeInsets.fromLTRB(25, 30, 25, 0),
                    child: SizedBox(
                      width: double.infinity,
                      child: CupertinoButton(
                          borderRadius: BorderRadius.all(Radius.circular(4)),
                          minSize: 35,
                          padding: EdgeInsets.all(0),
                          color: Colors.deepPurple,
                          child: Text(
                            'Add Courses',
                            style: TextStyle(
                              color: Colors.white,
                              fontSize: 16,
                            ),
                          ),
                          onPressed: () {
                            Navigator.push(
                                context,
                                MaterialPageRoute(
                                    builder: (context) => AddCourses()
                                )
                            );
                          }),
                    ),
                  ),
                  new Padding(
                    padding: EdgeInsets.fromLTRB(25, 60, 25, 30),
                    child: SizedBox(
                      width: double.infinity,
                      child: CupertinoButton(
                          borderRadius: BorderRadius.all(Radius.circular(4)),
                          minSize: 35,
                          padding: EdgeInsets.all(0),
                          color: Colors.deepPurple,
                          child: Text(
                            'Login',
                            style: TextStyle(
                              color: Colors.white,
                              fontSize: 16,
                            ),
                          ),
                          onPressed: () {
                            if (_sid.text.isEmpty ||
                                _gpa.text.isEmpty) {
                              showDialog(
                                  context: context,
                                  builder: (BuildContext context) =>
                                      CupertinoAlertDialog(
                                        title: Text("Oops!"),
                                        content: Text(
                                            "Please fill all required information"),
                                        actions: <Widget>[
                                          CupertinoDialogAction(
                                            isDefaultAction: true,
                                            child: Text("Got it"),
                                            onPressed: () {
                                              Navigator.of(context).pop();
                                            },
                                          )
                                        ],
                                      ));
                            } else {
                              //CHECK LW ELCOUNTER BTA3 COURSES >=4
                            }
                          }),
                    ),
                  ),
                ],
              )
            ])
        )
    );
  }
}