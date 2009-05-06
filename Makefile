.OBJDIR: ebin
.SUFFIXES: .erl .beam

.erl.beam:
	erlc -pz ebin -o ebin $<

MODS = src/manager
EXPS = examples/baby_mgr examples/baby

all: compile

compile: ${MODS:%=%.beam}

examples: ${EXPS:%=%.beam}

clean:
	rm -f ebin/*.beam
