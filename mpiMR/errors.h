/**************************************************************
*
* 	errors.h
* 	$Id: errors.h,v 1.1.1.1 2001/11/26 04:29:47 bostrick Exp $
*
* 	inspired by Butenhof, David R.
* 	Programming with Posix Threads
* 	Addison-Wesley, 1997, p. 33
*
* 	<bowe@redhat.com>
*
***************************************************************/

#ifndef __errors_h_
#define __errors_h_

#include <errno.h>
#include <stdio.h>
#include <string.h>

#ifdef DEBUG
# define DPRINTF(arg) printf arg
#else
# define DPRINTF(arg)
#endif

/*
*
* 	why the do { ... } while (0) ?
*
* 	the macro can be treated like a fcn call, even
* 	in contexts where a trailing ; would generate a null
* 	statement.
*
* 	if (status != 0)
* 		err_abort(status, "message");
* 	else
* 		return status;
*
* 	would be a syntax error otherwise.
*
* 	i'm not sure i can understand it, but i can copy it!  --bowe
*
*/


#define msg_abort(text) do {\
	fprintf(stderr, "%s at \"%s\":%d\n", \
		text, __FILE__, __LINE__); \
		abort(); \
	} while(0)

#define err_abort(code, text) do {\
	fprintf(stderr, "%s at \"%s\":%d: %s\n", \
		text, __FILE__, __LINE__, strerror(code)); \
		abort(); \
	} while(0)

#define errno_abort(text) do {\
	fprintf(stderr, "%s at \"%s\":%d: %s\n", \
		text, __FILE__, __LINE__, strerror(errno)); \
		abort(); \
	} while(0)


#endif /* _errors_h_ */

