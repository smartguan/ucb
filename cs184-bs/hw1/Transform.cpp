// Transform.cpp: implementation of the Transform class.

#include "stdafx.h"
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

mat4 Transform::lookAt(vec3 eye, vec3 up) {
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







Transform::Transform()
{

}

Transform::~Transform()
{

}

// Some notes about using glm functions.
// You are ONLY permitted to use glm::dot glm::cross glm::normalize
// Do not use more advanced glm functions (in particular, directly using 
// glm::lookAt is of course prohibited).  

// You may use overloaded operators for matrix-vector multiplication 
// But BEWARE confusion between opengl (column major) and row major 
// conventions, as well as what glm implements. 
// In particular, vecnew = matrix * vecold may not implement what you think 
// it does.  It treats matrix as column-major, in essence using the transpose.
// We recommend using row-major and vecnew = vecold * matrix 
// Preferrably avoid matrix-matrix multiplication altogether for this hw.  
