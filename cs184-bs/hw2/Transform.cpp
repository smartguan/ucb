// Transform.cpp: implementation of the Transform class.

//#include "stdafx.h"
#include "Transform.h"

//Takes as input the current eye position, and the current up vector.
//up is always normalized to a length of 1.
//eye has a length indicating the distance from the viewer to the origin

// Helper rotation function.  Please implement this.  

mat3 Transform::rotate(const float degrees, const vec3& axis) {
  mat3 R ; 
  // FILL IN YOUR CODE HERE
  float angR = degrees*pi/180; //Calculate for the radiun
  //Normalize the vecter
  float mag = sqrt(axis.x*axis.x + axis.y*axis.y + axis.z*axis.z);
  vec3 norR = axis/mag;
  
  //Rotate about norR
  R = mat3(1,0,0,0,1,0,0,0,1)*cos(angR)
	  + sin(angR)*mat3(0, -norR.z, norR.y,
	                   norR.z, 0, -norR.x,
					   -norR.y, norR.x, 0)
	  + (1-cos(angR))*mat3(norR.x*norR, norR.y*norR, norR.z*norR);
  return R ;
}

void Transform::left(float degrees, vec3& eye, vec3& up) {

	//FILL IN YOUR CODE HERE
	eye = rotate(degrees, -up)*eye;

}

void Transform::up(float degrees, vec3& eye, vec3& up) {

	//FILL IN YOUR CODE 
	vec3 temp = vec3(eye.y*up.z-eye.z*up.y,
				      eye.z*up.x-eye.x*up.z,
					  eye.x*up.y-eye.y*up.x);
	eye = rotate(degrees, -temp)*eye;
	up = rotate(degrees, -temp)*up;
}

mat4 Transform::lookAt(const vec3& eye, const vec3& center, const vec3& up) {
    mat4 M ; 

	//FILL IN YOUR CODE HERE
    //You must return a row-major mat4 M that you create from this routine
	vec3 z = glm::normalize(eye);
	vec3 x = glm::normalize(glm::cross(up, z));
	vec3 y = glm::cross(z, x);
	   
	//In row major                 right-axis       , magInRightDirection    
	//Form the translation matrix  up-axis          , magInUpDirection
	//                             depth-axis(out)  , magInOutDirection
	//                             0,0,0,1
	//Note: GL_MODELVIEW takes in column-major matrix
	M = mat4(x.x, x.y, x.z, -glm::dot(x, eye),
		     y.x, y.y, y.z, -glm::dot(y, eye),
			 z.x, z.y, z.z, -glm::dot(z, eye),
			 0, 0, 0, 1);

	return M ; 
}


//GL_perspective func
//row matrix          1/aspect, 0, 0, 0,    
//                    0, 1, 0, 0,
//                    0, 0, A, B,
//                    0, 0, -1/zNear, 0
//where A,B determines the near and far z (normally -1 -> 1)
mat4 Transform::perspective(float fovy, float aspect, float zNear, float zFar)
{
	float A=0, B=0, d=0;
	float angle = 3.1415926*fovy/360;

	//Calculate A, B, d
	A = -(zFar+zNear)/(zFar-zNear);
	B = -2*zFar*zNear/(zFar-zNear);
	d = cos(angle)/sin(angle);

	return mat4(d/aspect, 0, 0, 0,
				0, d, 0, 0,
				0, 0, A, B,
				0, 0, -1, 0);
}



//Scale func
mat4 Transform::scale(const float &sx, const float &sy, const float &sz) 
{
	return mat4(sx, 0, 0, 0, 
				0, sy, 0, 0,
				0, 0, sz, 0,
				0, 0, 0, 1);
}


//Translate func
mat4 Transform::translate(const float &tx, const float &ty, const float &tz)
{
	return mat4(1, 0, 0, tx,
				0, 1, 0, ty,
				0, 0, 1, tz,
				0, 0, 0, 1);
}




Transform::Transform()
{

}

Transform::~Transform()
{

}