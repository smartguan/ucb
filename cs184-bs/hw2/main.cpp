/*****************************************************************************/
/* This is the program skeleton for homework 2 in CS 184 by Ravi Ramamoorthi */
/* Extends HW 1 to deal with shading, more transforms and multiple objects   */
/*****************************************************************************/


#include <iostream>
#include <stack>
#include <fstream>
#include <vector> 
#include <sstream> 
#include <string>
#include <cmath>
#include <GL/glew.h>
#include <GL/glut.h>
#include "shaders.h"
#include "Transform.h"
using namespace std;



int winWidth, winLength;    //Window solution

int amount; // The amount of rotation for each arrow press

vec3 eye; // The (regularly updated) vector coordinates of the eye location 
vec3 up;  // The (regularly updated) vector coordinates of the up location 
vec3 center;
vec3 eyeinit ; // Initial eye position, also for resets
vec3 upinit; // Initial up position, also for resets
vec3 centerinit;
float fovy;
bool useGlu; // Toggle use of "official" opengl/glm transform vs user code
int w, h; // width and height 
GLuint vertexshader, fragmentshader, shaderprogram ; // shaders

static enum {view, translate, scale} transop ; // which operation to transform by 

int theObject = 0; //the object operated(0: global; 1~10:lights; 11~20:teapots; 21~30: cubes)
int nthLight=0;   //number of lights loaded 

//float sx, sy, sz; // the global scale in x and y 
//float tx, ty, tz; // the global translation in x and y
float s[31][3] = {1}; // the  scale in x and y z
float t[31][3] = {0}; // the  translation in x and y z

GLfloat light_position[10][4]; //use to init
GLfloat light_specular[10][4]; //use to init
GLfloat light_pos[10][4];	   //use except init
vec3 lightposvec[10];	//light vector
vec3 lightposup[10];    //light up vec

GLfloat matProp[4] = {0};							//material properties-amb,dif,spe
GLfloat matShiness[1] = {0};							//Shiness

const GLfloat one[] = {1, 1, 1, 1};                 // Specular on teapot
const GLfloat medium[] = {0.5, 0.5, 0.5, 1};		// Diffuse on teapot

const GLfloat small[] = {0.2, 0.2, 0.2, 1};         // Ambient on teapot
const GLfloat least[] = {0,0,0,1};
const GLfloat high[] = {100} ;						// Shininess of teapot
GLfloat light[10][4];

// Variables to set uniform params for lighting fragment shader 
GLuint islight ; 
GLuint lightPosn[10];
GLuint lightColor[10];

GLuint ambient ; 
GLuint diffuse ; 
GLuint specular ; 
GLuint shininess ; 

stack<mat4> transformStack; //Matrix stack
string objDrawCommands = "";   //Commands for drawing all objects
float deg = 0;  //ratation angle of lights
bool animate = false;    //toggle of animation


//File reader, collect data
//Vector template
std::vector<std::string> &split(const std::string &s, char delim, std::vector<std::string> &elems) { 
    std::stringstream ss(s); 
    std::string item; 
    while(std::getline(ss, item, delim)) { 
        elems.push_back(item); 
    } 
    return elems; 
} 
 
 
std::vector<std::string> split(const std::string &s, char delim) { 
    std::vector<std::string> elems; 
    return split(s, delim, elems); 
} 

//string to float
float stf(string in) {
	float result;
	stringstream ss (stringstream::in | stringstream::out);
	ss << in;
	ss >> result;
	return result;
}


string initData (const char * filename) {
  int nthObj = 0, nthTransform = 0,  index=0;
  string str, ret = "" ; 
  ifstream in ; 
  std::vector<std::string> x;   //Vector object used to split the input string
  in.open(filename) ; 
  if (in.is_open()) {
    getline (in, str) ; 
    while (in) {
		if ((str.find_first_not_of("\t\r\n") != string::npos) && (str[0] != '#')) {
			str = str.substr(str.find_first_not_of(" "));
			x = split(str, ' ');
			
			//Window size
			if(x[0] == "size") {
				winWidth = (int)stf(x[1]);
				winLength = (int)stf(x[2]);
			}
			//Camera
			else if (x[0] == "camera") {
				eyeinit = vec3(stf(x[1]), stf(x[2]), stf(x[3]));
				centerinit = vec3(stf(x[4]), stf(x[5]), stf(x[6]));
				upinit = vec3(stf(x[7]), stf(x[8]), stf(x[9]));
				fovy = stf(x[10]);
			}
			//Lights
			else if(x[0] == "light") {
				for(index=0; index<4; index++) {
					light_position[nthLight][index] = stf(x[1+index]);
					light_specular[nthLight][index] = stf(x[5+index]);
				}
				nthLight++;
			}
			//Objects drawing commands
			else {
				ret = ret + str + "\n";
			}
			
		}	
		getline (in, str) ; 	
    }

    return ret ; 
  }
  else {
    cerr << "Unable to Open File " << filename << "\n" ; 
    throw 2 ; 
  }
}






// New helper transformation function to transform vector by modelview 
// May be better done using newer glm functionality.
void transformvec (const GLfloat input[4], GLfloat output[4]) {
  GLfloat modelview[16] ; // in column major order
  glGetFloatv(GL_MODELVIEW_MATRIX, modelview) ; 
  
  for (int i = 0 ; i < 4 ; i++) {
    output[i] = 0 ; 
    for (int j = 0 ; j < 4 ; j++) 
      output[i] += modelview[4*j+i] * input[j] ; 
  }
}

// Uses the Projection matrices (technically deprecated) to set perspective 
// We could also do this in a more modern fashion with glm.  
void reshape(int width, int height){
	w = width;
	h = height;
        mat4 mv ; // just like for lookat

	glMatrixMode(GL_PROJECTION);
        float aspect = w / (float) h, zNear = 0.1, zFar = 90.0;
        // I am changing the projection stuff to be consistent with lookat
        if (useGlu) mv = glm::perspective(fovy,aspect,zNear,zFar) ; 
        else {
          mv = Transform::perspective(fovy,aspect,zNear,zFar) ; 
          mv = glm::transpose(mv) ; // accounting for row major 
        }
        glLoadMatrixf(&mv[0][0]) ; 

	glViewport(0, 0, w, h);
}


void animation() {
	if(abs(deg) >= 360) deg = 0;
	deg = abs(deg) + 20;
	
	glutPostRedisplay() ;
}



void printHelp() {
  std::cout << "\npress 'h' to print this message again.\n" 
       << "press '+' or '-' to change the amount of rotation that\noccurs with each arrow press.\n" 
            << "press 'g' to switch between using glm::lookAt and glm::Perspective or your own LookAt.\n"       
            << "press 'r' to reset the transformations.\n"
            << "press 'v' 't' 's' to do view [default], translate, scale.\n"
			<< "press 'p' to toggle animation\n"
			<< "press 'o' to switch to global operation.\n"
            << "press ESC to quit.\n" ;  
    
}

void keyboard(unsigned char key, int x, int y) {
	int i=0, j=0;
	switch(key) {
	case '+':
		amount++;
		std::cout << "amount set to " << amount << "\n" ;
		break;
	case '-':
		amount--;
		std::cout << "amount set to " << amount << "\n" ; 
		break;
	case 'g':
		useGlu = !useGlu;
                reshape(w,h) ; 
		std::cout << "Using glm::LookAt and glm::Perspective set to: " << (useGlu ? " true " : " false ") << "\n" ; 
		break;
	case 'p': // ** NEW ** to pause/restart animation
		animate = !animate ;
		cout << "Toggle animating.\n";
		if (animate) glutIdleFunc(animation) ;
		else glutIdleFunc(NULL) ;
		break ;
	case 'h':
		printHelp();
		break;
        case 27:  // Escape to quit
            exit(0) ;
            break ;
        case 'r': // reset eye and up vectors, scale and translate. 
			eye = eyeinit ; 
			up = upinit ; 
			theObject = 0;
			for(i=0;i<31;i++) {
				for(j=0; j<3; j++) {
					s[i][j] = 1;
					t[i][j] = 0;
				}
			}
			for(i=0; i<10; i++) {
				//if not diretional light
				if(light_position[i][3] != 0)  
					lightposvec[i] = vec3(light_position[i][0], light_position[i][1], light_position[i][2]);
				else lightposvec[i] = vec3(-light_position[i][0], -light_position[i][1], -light_position[i][2]);
				lightposup[i] = up;
			}
			std::memcpy(light_pos, light_position, sizeof(light_position)+1);
			
			break ;   
        case 'v': 
            transop = view ;
            std::cout << "Operation is set to View\n" ; 
            break ; 
        case 't':
            transop = translate ; 
            std::cout << "Operation is set to Translate\n" ; 
            break ; 
        case 's':
            transop = scale ; 
            std::cout << "Operation is set to Scale\n" ;
			break;
		case 'o':
			theObject = 0;
			std::cout << "Global operation selected.\n" ;
			break;
		case '0':
			if(nthLight < 0+1) std::cout << "No such light.\n";
			else {
				theObject = 1;
				std::cout << "Light0 selected.\n" ;
			}
			break;
		case '1':
			if(nthLight < 1+1) std::cout << "No such light.\n";
			else {
				theObject = 2;
				std::cout << "Light1 selected.\n" ;
			}
			break;
		case '2':
			if(nthLight < 2+1) std::cout << "No such light.\n";
			else {
				theObject = 3;
				std::cout << "Light2 selected.\n" ;
			}
			break;
		case '3':
			if(nthLight < 3+1) std::cout << "No such light.\n";
			else {
				theObject = 4;
				std::cout << "Light3 selected.\n" ;
			}
			break;
		case '4':
			if(nthLight < 4+1) std::cout << "No such light.\n";
			else {
				theObject = 5;
				std::cout << "Light4 selected.\n" ;
			}
			break;
		case '5':
			if(nthLight < 5+1) std::cout << "No such light.\n";
			else {
				theObject = 6;
				std::cout << "Light5 selected.\n" ;
			}
			break;
		case '6':
			if(nthLight < 6+1) std::cout << "No such light.\n";
			else {
				theObject = 7;
				std::cout << "Light6 selected.\n" ;
			}
			break;
		case '7':
			if(nthLight < 7+1) std::cout << "No such light.\n";
			else {
				theObject = 8;
				std::cout << "Light7 selected.\n" ;
			}
			break;
		case '8':
			if(nthLight < 8+1) std::cout << "No such light.\n";
			else {
				theObject = 9;
				std::cout << "Light8 selected.\n" ;
			}
			break;
		case '9':
			if(nthLight < 9+1) std::cout << "No such light.\n";
			else {
				theObject = 10;
				std::cout << "Light9 selected.\n" ;
			}
			break;

	}
	glutPostRedisplay();
}

//  You will need to enter code for the arrow keys 
//  When an arrow key is pressed, it will call your transform functions

void specialKey(int key, int x, int y) {
	switch(key) {
	case 100: //left
          if (transop == view) {
			if(theObject == 0) Transform::left(-amount, eye,  up);
			else Transform::left(-amount, lightposvec[theObject-1],  lightposup[theObject-1]);
		  }
          else if (transop == scale) s[theObject][0] -= amount * 0.01 ; 
          else if (transop == translate) t[theObject][0] -= amount * 0.01 ; 
          break;
	case 101: //up
          if (transop == view) {
			if(theObject == 0) Transform::up(amount, eye,  up);
			else Transform::up(amount, lightposvec[theObject-1],  lightposup[theObject-1]);
		  }
          else if (transop == scale) s[theObject][1] += amount * 0.01 ; 
          else if (transop == translate) t[theObject][1] += amount * 0.01 ; 
          break;
	case 102: //right
          if (transop == view) {
			if(theObject == 0) Transform::left(amount, eye,  up);
			else Transform::left(amount, lightposvec[theObject-1],  lightposup[theObject-1]);
		  }
          else if (transop == scale) s[theObject][0] += amount * 0.01 ; 
          else if (transop == translate) t[theObject][0] += amount * 0.01 ; 
          break;
	case 103: //down
          if (transop == view) {
			if(theObject == 0) Transform::up(-amount, eye,  up);
			else Transform::up(-amount, lightposvec[theObject-1],  lightposup[theObject-1]);
		  }
          else if (transop == scale) s[theObject][1] -= amount * 0.01 ; 
          else if (transop == translate) t[theObject][1] -= amount * 0.01 ; 
          break;
	}


	glutPostRedisplay();
}



void init() {
  
	objDrawCommands = initData("demo.txt");    //Read the data file, store the draw commands
  // Set up initial position for eye, up and amount
  // As well as booleans 
	
    eye = eyeinit ; 
	up = upinit ; 
	center = centerinit;
	amount = 5;
	
	int m=0, n=0;
	//init light vec
	for(m=0; m<10; m++) {
		//if not diretional light
		if(light_position[m][3] != 0)  
			lightposvec[m] = vec3(light_position[m][0], light_position[m][1], light_position[m][2]);
		else lightposvec[m] = vec3(-light_position[m][0], -light_position[m][1], -light_position[m][2]);
		lightposup[m] = up;
	}
	std::memcpy(light_pos, light_position, sizeof(light_position)+1);
	
	//init scale and translate
    for(m=0;m<31;m++) {
		for(n=0; n<3; n++) {
			s[m][n] = 1;
			t[m][n] = 0;
		}
	}
	useGlu = true;
	transformStack.push(mat4(1.0));

	glEnable(GL_DEPTH_TEST);

  // The lighting is enabled using the same framework as in mytest 3 
  // Except that we use two point lights
  // For now, lights and materials are set in display.  Will move to init 
  // later, per update lights

      vertexshader = initshaders(GL_VERTEX_SHADER, "shaders/light.vert.glsl") ;
      fragmentshader = initshaders(GL_FRAGMENT_SHADER, "shaders/light.frag.glsl") ;
      shaderprogram = initprogram(vertexshader, fragmentshader) ; 
      islight = glGetUniformLocation(shaderprogram,"islight") ;   
	  
	  int i =0;
	  char p[20] = "pointLightPosn[";
	  char c[20] = "pointLightColor[";
	  for(i=0; i<10; i++) {
		  p[15]='0'+i;
		  p[16] = ']';
		  c[16] = p[15];
		  c[17] = p[16];
		 
		  lightPosn[i] = glGetUniformLocation(shaderprogram, p) ;       
		  lightColor[i] = glGetUniformLocation(shaderprogram,c) ;  
	  }

      ambient = glGetUniformLocation(shaderprogram,"ambient") ;    
      diffuse = glGetUniformLocation(shaderprogram,"diffuse") ;       
      specular = glGetUniformLocation(shaderprogram,"specular") ;       
      shininess = glGetUniformLocation(shaderprogram,"shininess") ;     


}

void display() {
	glClearColor(0, 0, 1, 0);
	glClear(GL_COLOR_BUFFER_BIT | GL_DEPTH_BUFFER_BIT);


	glMatrixMode(GL_MODELVIEW);
	mat4 mv ; 

    if (useGlu) mv = glm::lookAt(eye,center,up) ; 
	else {
          mv = Transform::lookAt(eye,center,up) ; 
          mv = glm::transpose(mv) ; // accounting for row major
        }
    glLoadMatrixf(&mv[0][0]) ; 

        // Set Light and Material properties for the teapot
        // Lights are transformed by current modelview matrix. 
        // The shader can't do this globally. 
        // So we need to do so manually.  

    // Transformations for Teapot, involving translation and scaling 
    mat4 sc(1.0) , tr(1.0) ; 
    sc = Transform::scale(s[0][0],s[0][1],s[0][2]) ; 
    tr = Transform::translate(t[0][0],t[0][1],t[0][2]) ; 
    // Multiply the matrices, accounting for OpenGL and GLM.
    sc = glm::transpose(sc) ; tr = glm::transpose(tr) ; 
    mat4 transf  = mv * tr * sc ; // scale, then translate, then lookat.
    glLoadMatrixf(&transf[0][0]) ; 

	//Update the matrix stack
	transformStack.push(transf);
	

	//Draw
	stringstream drawComss(objDrawCommands);	
	string str = "";

	std::vector<std::string> x;   //Vector object used to split the current string
	int i=0;  //iterator

	//rotate, translate and scale matrix
	mat4 rot(mat4(1)), tran(mat4(1)), sca(mat4(1));

	//Light
	glUniform4fv(ambient,1,small) ; 
    glUniform4fv(diffuse,1,small) ; 
    glUniform4fv(specular,1,one) ; 
	glUniform1fv(shininess,1,high) ; 
	glUniform1i(islight,true) ;


	mat4 mvl ; 
	// Transformations for Teapot, involving translation and scaling 
	mat4 scl(1.0) , trl(1.0) ; 

	for(i=0; i<10 && i<nthLight;i++) {
		    transformStack.push(transformStack.top());

			scl = Transform::scale(s[i+1][0],s[i+1][1],s[i+1][2]) ; 
			trl = Transform::translate(t[i+1][0],t[i+1][1],t[i+1][2]) ;

			//for animation
			deg = -1*deg;
			rot = glm::transpose(mat4(Transform::rotate(deg, upinit)));

			// Multiply the matrices, accounting for OpenGL and GLM.
			scl = glm::transpose(scl) ; trl = glm::transpose(trl) ; 
			transformStack.top()  = transformStack.top()*trl * rot * scl ; // scale, then translate, then lookat.
			glLoadMatrixf(&transformStack.top()[0][0]) ; 
			
			//change the position of point/directional light
			if(light_pos[i][3] != 0) {
				light_pos[i][0] = lightposvec[i].x;
				light_pos[i][1] = lightposvec[i].y;
				light_pos[i][2] = lightposvec[i].z;
			}
			else{
				light_pos[i][0] = -lightposvec[i].x;
				light_pos[i][1] = -lightposvec[i].y;
				light_pos[i][2] = -lightposvec[i].z;
			}
			

			//passing the light to the shader
			transformvec(light_pos[i], light[i]) ; 

			glUniform4fv(lightPosn[i], 1, light[i]) ; 
			glUniform4fv(lightColor[i], 1, light_specular[i]) ;
	/*
			rot = glm::transpose(mat4(Transform::rotate(deg, vec3(0,1,1))));
			tran = glm::transpose(Transform::translate(0, 0, 0));
			transformStack.top() = transformStack.top()*tran*rot;
			glLoadMatrixf(&transformStack.top()[0][0]);
			for(i=0; i<10;i++) {
				transformvec(light_position[i], light[i]) ; 
	
				glUniform4fv(lightPosn[i], 1, light[i]) ; 
				glUniform4fv(lightColor[i], 1, light_specular[i]) ; 
			}
		
	*/
		transformStack.pop();
		glLoadMatrixf(&transformStack.top()[0][0]);
	}

	 
	
	//Other objects
	getline(drawComss, str);
	while(drawComss) {
//		cout << str << "\n";
		x = split(str, ' ');
		if(x[0] == "pushTransform") transformStack.push(transformStack.top());
		else if(x[0] == "popTransform") {
			transformStack.pop();
			glLoadMatrixf(&transformStack.top()[0][0]);
		}
		//Get ambient
		else if(x[0] == "ambient") {
			for(i=0;i<4;i++) matProp[i] = stf(x[i+1]);
			glUniform4fv(ambient,1,matProp) ;
		}
		//Get diffuse
		else if(x[0] == "diffuse") {
			for(i=0;i<4;i++) matProp[i] = stf(x[i+1]);
			glUniform4fv(diffuse,1,matProp) ;
		}
		//Get specular
		else if(x[0] == "specular") {
			for(i=0;i<4;i++) matProp[i] = stf(x[i+1]);
			glUniform4fv(specular,1,matProp) ;
		}
		//Get shininess
		else if(x[0] == "shininess") {
			matShiness[0] = stf(x[1]);
			glUniform4fv(shininess,1,matShiness) ;
		}
		//Get translate
		else if(x[0] == "translate") {
			tran = glm::transpose(Transform::translate(stf(x[1]), stf(x[2]), stf(x[3])));
//			cout << "tran: : " << stf(x[3]) << "\n";
			transformStack.top() = transformStack.top()*tran;
			glLoadMatrixf(&transformStack.top()[0][0]);
		}
		//Get rotation
		else if(x[0] == "rotate") {
			rot = glm::transpose(mat4(Transform::rotate(stf(x[4]), vec3(stf(x[1]),stf(x[2]),stf(x[3])))));
//			cout << "rot: " << stf(x[4]) << "\n";
			transformStack.top() = transformStack.top()*rot;
			glLoadMatrixf(&transformStack.top()[0][0]);
		}
		//Get scale
		else if(x[0] == "scale") {
			sca = glm::transpose(Transform::scale(stf(x[1]), stf(x[2]), stf(x[3])));
//			cout << "sca: " << stf(x[3]) << "\n";
			transformStack.top() = transformStack.top()*sca;
			glLoadMatrixf(&transformStack.top()[0][0]);
		}
		//Draw teapot
		else if(x[0] == "teapot") glutSolidTeapot(stf(x[1]));
		//Draw cube
		else if(x[0] == "cube") glutSolidCube(stf(x[1]));
		//Draw teapot
		else if(x[0] == "sphere") glutSolidSphere(stf(x[1]), 100, 100);

		
		getline(drawComss, str);
	}

	glutSwapBuffers();
}

int main(int argc, char* argv[]) {
	glutInit(&argc, argv);
	glutInitDisplayMode(GLUT_DOUBLE | GLUT_RGBA | GLUT_DEPTH);
	glutCreateWindow("HW2: Shaders");

	//glew init
	GLenum err = glewInit();
//	if (GLEW_OK != err) { 
//		std::cerr << "Error: " << glewGetString(err) << "\n";
//	}

	init();
	glutDisplayFunc(display);
	glutSpecialFunc(specialKey);
	glutKeyboardFunc(keyboard);
	glutReshapeFunc(reshape);
	glutReshapeWindow(winWidth, winLength);
	printHelp();
	glutMainLoop();
	return 0;
}
