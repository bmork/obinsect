#include <stdio.h>
#include <unistd.h>
#include <poll.h>

#define TIMEOUT -1

int main (void)
{
	unsigned char buf[16];
	struct pollfd fds[1];
	int ret, rlen;

	/* watch stdin for input */
	fds[0].fd = STDIN_FILENO;
	fds[0].events = POLLIN;

	while (1) {
		ret = poll(fds, 1, TIMEOUT * 1000);
		if (ret == -1) {
			perror ("poll");
			return 1;
		}
		if (!ret) {
			fprintf(stderr, "%d seconds elapsed.\n", TIMEOUT);
			return 0;
		}

		if (fds[0].revents & POLLIN)
			fprintf(stderr, "stdin is readable\n");

		rlen = read(fds[0].fd, buf, sizeof(buf));
		if (!rlen)
			break;
		fprintf(stderr, "read %d bytes\n", rlen);
	}
	fprintf(stderr, "eof\n");
	return 0;

}
